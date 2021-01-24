  -- merge_new_records.sql
  -- Inserts new csv records into the appropriate history table.
  -- Uses a BigQuery table that is linked to the relevant
  -- tastworks csv history file located on the G-Drive.
  -- In addition to merging new records, also performs calculated
  -- field computations described below.
  -- The MERGE only inserts records in the csv file that are not
  -- in the current history table
MERGE
  `<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>` AS T --insertion here******
USING
  (
  SELECT
    --The original CSV fields
    Date,
    Type,
    Action,
    Symbol,
    Instrument_Type,
    Description,
    Value,
    Quantity,
    Average_Price,
    Commissions,
    Fees,
    Multiplier,
    Underlying_Symbol,
    Expiration_Date,
    Strike_Price,
    Call_or_Put,
    SAFE_CAST(Order_Number AS STRING),
    --Begin calculated field computations.
    --For equities the tasty csv file leaves the Underlying Symbol column NULL.
    --For ease, this creates a single column that references the underlying symbol
    --for a given row and avoids having to look at 2 different columns to discern
    --the underlying of that row.
  IF
    (Instrument_Type = "Equity",
      Symbol,
      Underlying_Symbol) AS Adjusted_Underlying_Symbol,
    --For equities the tasty csv file leaves the multiplier column NULL.
    --For ease, this creates a single column that explicitly references the multiplier
    --for a given row with equities assigned a 1 value multiplier.  The multiplier indicates
    --the number of shares each unit in the quantity column represents.  For example, a multiplier
    --of 100 means a quantity of 3 represent 300 shares.
    --The multiplier can be used to transform the quantity column to a number of shares basis.
  IF
    (Instrument_Type = "Equity",
      1,
      Multiplier) AS Adjusted_Multiplier,
    --Create a buy/sell flag column that denotes the transaction as a buy (+) or sell (-) along with the quantity bought or sold
    --Zero is assigned for ambiguous row-wise cases; this occurs for Receive Deliver transactions where options are removed due to expiration
    --There is no way to tell from the row transaction whether the removal was effectually a buy or sell
    --A removal would be effectually a sell if a portfolio is net long the symbol and a buy if net short the symbol; this is addressed in the next procedure
    --Summing the buy/sell flag grouped by symbol tells you if a portfolio is long (>0), short (<0) or flat (=0) that symbol
    CASE
      WHEN (Action = "BUY_TO_OPEN" OR ACTION = "BUY_TO_CLOSE" OR Description = "Removal of option due to assignment") THEN 1*Quantity/ IF (Instrument_Type = "Equity", 100, 1) --converts buy sell flag for equities to a per lot basis
      WHEN (Action = "SELL_TO_OPEN"
      OR ACTION = "SELL_TO_CLOSE") THEN -1*Quantity/
  IF
    (Instrument_Type = "Equity",
      100,
      1) --converts buy sell flag for equities to a per lot basis
      WHEN Type = "Receive Deliver" AND ACTION IS NULL THEN 0
    ELSE
    NULL
  END
    AS Buy_Sell_Flag,
    --Create a open/close flag column that denotes the transaction as an open (+) or close (-) along with the quantity opened or closed
    --For Receive Deliver type transactions where the Action column is NULL use the description column
    --The Description column will show either Received... or Removal... with received effectually an 'Open' and removal a 'Close'
    --Summing the open/close flag grouped by symbol tells you if a portfolio is open (>0) or flat (=0) that symbol
    --Summing the open/close flag grouped by Play_Id tells you if the play is open or closed; note the sum can never be <0
    --There is no row-wise ambiguity and the open/close flag can be determined solely from the row data
    CASE
      WHEN (Action = "BUY_TO_OPEN" OR ACTION = "SELL_TO_OPEN" OR LEFT(Description, 8) = "Received") THEN 1*Quantity/ IF (Instrument_Type = "Equity", 100, 1) --converts buy sell flag for equities to a per lot basis
      WHEN (Action = "BUY_TO_CLOSE"
      OR ACTION = "SELL_TO_CLOSE"
      OR LEFT(Description,7) = "Removal") THEN -1*Quantity/
  IF
    (Instrument_Type = "Equity",
      100,
      1) --converts buy sell flag for equities to a per lot basis
    ELSE
    NULL
  END
    AS Open_Close_Flag,
    "Unassigned" AS Play_Id,
    "<INSERT_SOURCE_ACCOUNT>" AS Account, --insertion here******
    "" AS Notes,
    CURRENT_TIMESTAMP() AS Last_Update
  FROM
    `<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_LINKED_CSV_TABLE_NAME>`) AS S --insertion here******
ON
  T.Date = S.Date
  AND T.Description = S.Description
  AND T.Value = S.Value
  WHEN NOT MATCHED BY TARGET
  THEN
INSERT
  ROW;
  
  
  -- For ambiguous receive deliver transactions, update the buy/sell flags
  -- with a zero value (assigned above). Ambiguous transactions occur
  -- when options are removed due to exercise.  This requires looking
  -- at the entire portfolio to see if the portfolio is net long or net short
  -- the given symbol.  The position status flag computed below is either
  -- a 0, 1 or -1 to denote flat, long or short respectively.
UPDATE
  `<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>` --insertion here******
  -- Set the buy/sell flag to the opposite of the current position status
  -- flag multipled by the quantity removed in the transaction.
  -- The transaction quantity removed will always be less than or equal to
  -- the current portfolios net quantity
SET
  Buy_Sell_Flag = -1*Quantity*
  -- Get the current position status of long (1) or short (-1) for each
  -- symbol with a buy/sell flag of 0.
  (
  SELECT
    Long_Short
  FROM
    -- Subquery returns a table of symbols and position statuses.
    -- A position status of 1 denotes long and -1 denotes short.
    (
    SELECT
      Symbol,
      SUM(Buy_Sell_Flag) AS Position_Status_Flag,
      SAFE_CAST(SUM(Buy_Sell_Flag)/ABS(SUM(Buy_Sell_Flag)) AS INT64) AS Long_Short
    FROM
      `<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>` --insertion here******
    GROUP BY
      Symbol
    HAVING
      Position_Status_Flag <> 0 )
  WHERE
    `<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>.Symbol` = Symbol --insertion here****** -- match parent query with subquery by symbol
    )
WHERE
  Buy_Sell_Flag = 0; -- set only rows with a zero buy/sell flag
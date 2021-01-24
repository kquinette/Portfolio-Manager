-- history_sheet_update.sql
SELECT 
  FORMAT_TIMESTAMP("%F %X %Z", Date), 
  Type, 
  Action, 
  Symbol, 
  Instrument_Type,
  Description,
  Value, Quantity, 
  Average_Price,
  Commissions,
  Fees, 
  Multiplier,
  Underlying_Symbol, 
  Expiration_Date, 
  Strike_Price, 
  Call_or_Put, 
  Order_Number, 
  Adjusted_Underlying_Symbol, 
  Adjusted_Multiplier, 
  Buy_Sell_Flag, 
  Open_Close_Flag, 
  Play_Id, Account 
FROM  `<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>`
ORDER BY Date DESC;
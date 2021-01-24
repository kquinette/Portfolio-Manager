-- assign_play_ids_view_open_play_ids.sql
SELECT 
	* 
EXCEPT 
	(Adjusted_Underlying_Symbol, Date) 
FROM 
	(SELECT 
		Adjusted_Underlying_Symbol, 
		Date, 
		Play_Id, 
		IF(Play_Id = "Unassigned", "Unassigned", IF(SUM(Open_Close_Flag) OVER (PARTITION BY Play_Id ORDER BY Play_Id) > 0, "Open", "Closed")) AS Status, 
		Notes, 
		FORMAT_TIMESTAMP("%F %X %Z", Date), 
		Description, 
		Expiration_Date, 
		Strike_Price, 
		Quantity, 
		Value, 
		IF(SUM(Open_Close_Flag) OVER (PARTITION BY Date ORDER BY Date) > 0, 
			"Opening Trade", 
			IF(SUM(Open_Close_Flag) OVER (PARTITION BY Date ORDER BY Date) < 0, 
				"Closing Trade", "Rolling Trade")) AS Trade_Type, 
			CASE 
				WHEN Buy_Sell_Flag > 0 AND Open_Close_Flag > 0  
					THEN "BUY_TO_OPEN" 
				WHEN Buy_Sell_Flag < 0 AND Open_Close_Flag < 0  
					THEN "BUY_TO_CLOSE" 
				WHEN Buy_Sell_Flag < 0 AND Open_Close_Flag > 0  
					THEN "SELL_TO_OPEN" 
				WHEN Buy_Sell_Flag < 0 AND Open_Close_Flag < 0  
					THEN "SELL_TO_CLOSE" 
				ELSE 
					NULL END, 
			Type, 
			Instrument_Type, 
			Account 
	FROM 
		`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>`
	WHERE 
		Adjusted_Underlying_Symbol IS NOT NULL) 
WHERE 
	STATUS != "Closed" 
	AND 
	Adjusted_Underlying_Symbol = "<INSERT_SYMBOL_HERE>" 
ORDER BY Adjusted_Underlying_Symbol, 
	Date DESC;
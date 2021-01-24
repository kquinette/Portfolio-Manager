-- assign_play_ids_update_play_id_dropdowns.sql
SELECT 
	CONCAT(Row_Number() Over(PARTITION BY Adjusted_Underlying_Symbol Order By Date), "-", Adjusted_Underlying_Symbol, "-", Date) 
FROM 
	`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>`
WHERE 
	Adjusted_Underlying_Symbol IS NOT NULL 
	AND 
	Adjusted_Underlying_Symbol = "<INSERT_SYMBOL_HERE>" 
GROUP BY 
	Date, 
	Adjusted_Underlying_Symbol 
HAVING 
	SUM(Open_Close_Flag) > 0 
ORDER BY 
	Adjusted_Underlying_Symbol, 
	Date DESC;
-- assign_play_ids_update_symbol_dropdowns_closed.sql
--closed symbols only
SELECT DISTINCT 
	Adjusted_Underlying_Symbol 
FROM 
	`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>`
WHERE 
	Adjusted_Underlying_Symbol IS NOT NULL 
GROUP BY 
	Adjusted_Underlying_Symbol 
HAVING 
	SUM(Open_Close_Flag) = 0 
ORDER BY Adjusted_Underlying_Symbol;

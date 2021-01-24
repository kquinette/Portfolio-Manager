-- assign_play_ids_update_symbol_dropdowns_all.sql
--all symbols
SELECT DISTINCT 
	Adjusted_Underlying_Symbol 
FROM 
	`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>`
WHERE 
	Adjusted_Underlying_Symbol IS NOT NULL 
ORDER BY 
	Adjusted_Underlying_Symbol;
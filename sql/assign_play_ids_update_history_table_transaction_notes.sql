UPDATE 
	`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>` AS A 
SET 
	A.Notes = B.Notes, A.Last_Update = CURRENT_TIMESTAMP() 
FROM 
	`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_LINKED_SHEET_TABLE_NAME>` AS B 
WHERE 
	A.Date = B.Date 
	AND 
	A.Account = B.Account 
	AND 
	A.Description = B.Description 
	AND 
	A.Adjusted_Underlying_Symbol IS NOT NULL;
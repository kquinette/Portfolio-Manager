-- closed_plays_play_id_transactions.sql
SELECT 
	FORMAT_TIMESTAMP("%F %X %Z", Date) AS Date, 
	Description, 
	Value/100 AS Credit, 
	(Value + IF(SAFE_CAST(Commissions AS FLOAT64) IS NULL, 0, SAFE_CAST(Commissions AS FLOAT64)) + Fees)/100 AS Credit_after_Fees  
FROM 
	`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>` --insertion here******
WHERE 
	Play_Id = "<INSERT_PLAY_ID_HERE>" --insertion here******
ORDER BY 
	Date DESC;
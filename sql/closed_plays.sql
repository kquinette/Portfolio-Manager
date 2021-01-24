-- closed_plays.sql
SELECT * FROM
	(SELECT Play_Id, 
			FORMAT_TIMESTAMP("%F %X %Z", MAX(Date)) AS Closed_Date, 
			SUM(Value/(100)) AS Credits,  
			SUM((Value + If(SAFE_CAST(Commissions AS FLOAT64)IS NULL, 0, SAFE_CAST(Commissions AS FLOAT64)) + Fees)/100) AS Credits_w_Fees, 
			SUM((Value + If(SAFE_CAST(Commissions AS FLOAT64)IS NULL, 0, SAFE_CAST(Commissions AS FLOAT64)) + Fees)) AS PL 
	FROM 
		`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>` --insertion here******
	WHERE 
			Play_Id != "Stock" 
		AND
			Play_Id != "Other" 
		AND
			Adjusted_Underlying_Symbol IS NOT NULL 
		AND 
			REGEXP_CONTAINS(Adjusted_Underlying_Symbol, r"<INSERT_SYMBOL_HERE>") --insertion here******
	GROUP BY 
		Adjusted_Underlying_Symbol, Play_Id 
	HAVING 
		SUM(Open_Close_Flag) = 0) AS A
LEFT JOIN
   	(SELECT 
		Play_Id, Notes 
	FROM 
		`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_PLAY_ID_NOTES_TABLE_NAME>`) AS B --insertion here******
USING 
	(Play_Id)
ORDER BY 
	A.Closed_Date DESC, Play_Id;
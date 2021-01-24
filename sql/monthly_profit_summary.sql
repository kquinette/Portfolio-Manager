SELECT 
	Year_Mon, 
	SUM(Total_Value) AS PL, 
	SUM(Total_Value_After_Fees) AS PL_After_Fees, 
	SUM(Total_Value) - SUM(Total_Value_After_Fees) AS Fees 
FROM 
	(SELECT 
		Adjusted_Underlying_Symbol, 
		Account, Play_Id, MAX(Date) AS Close_Date, 
		FORMAT_TIMESTAMP("%b-%y", MAX(Date)) AS Year_Mon, 
		SUM(Value) AS Total_Value, 
		SUM((Value + If(SAFE_CAST(Commissions AS FLOAT64) IS NULL, 0, SAFE_CAST(Commissions AS FLOAT64)) + Fees)) AS Total_Value_After_Fees, 
		SUM(Open_Close_Flag) AS NOCF 
	FROM 
		`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>`
	WHERE 
			Play_Id != "Stock" 
		AND
			Play_Id != "Other" 
	    AND
			Adjusted_Underlying_Symbol IS NOT NULL 
		AND 
			Open_Close_Flag != 0 
	GROUP BY 
		Account,
		Adjusted_Underlying_Symbol,
		Play_Id
	HAVING 
		NOCF = 0) 
GROUP BY
	Year_Mon 
ORDER BY 
	MAX (Close_Date) LIMIT 12;
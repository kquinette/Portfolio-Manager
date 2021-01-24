--, "Underlying",	"Last Transaction Date",	"Play Id",	"Credits after Fees", "50% Target GTC",	"25% Target GTC",	"Credits before Fees",	"Initial Credit", "Legs",	"Position", "Expirations", "Spread", "Invert", "Play Id Notes", "Transaction Date", "Transaction History Description", "Credit", "Credit after Fees"

SELECT 
	A.Adjusted_Underlying_Symbol,
	FORMAT_TIMESTAMP("%D", A.Last_Date) AS Date, 
	A.Play_Id, 
	A.Credits_w_Fees,
	A.Credits - .5*B.Initial_Credit AS Fifty_Pct_GTC, 
	A.Credits - .25*B.Initial_Credit AS Twenty_Five_Pct_GTC, 
	A.Credits,  
	B.Initial_Credit, 
	A.Legs,
	C.Position, 
	Expirations, 
	Spread, 
	Invert,  
	Notes
FROM 
	(SELECT 
		Adjusted_Underlying_Symbol, 
		Play_Id, 
		SUM(Open_Close_Flag) AS Legs, 
		SUM(Value/100) AS Credits,  
		SUM((Value + If(SAFE_CAST(Commissions AS FLOAT64)IS NULL, 0, SAFE_CAST(Commissions AS FLOAT64)) + Fees)/100) AS Credits_w_Fees,
		FIRST_VALUE(SUM(Value/100)) OVER (PARTITION BY PLay_Id ORDER BY Adjusted_Underlying_Symbol) AS First_Credit,
		MAX(Date) AS Last_Date
	FROM 
		`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>`
	WHERE 
		Adjusted_Underlying_Symbol IS NOT NULL 
	GROUP BY 
		Adjusted_Underlying_Symbol, Play_Id 
	HAVING 
		SUM(Open_Close_Flag) > 0 
	ORDER BY 
		Adjusted_Underlying_Symbol, Play_Id) AS A

INNER JOIN

	(SELECT DISTINCT 
		Adjusted_Underlying_Symbol, 
		Play_Id,
		FIRST_VALUE(SUM(Value/100)) OVER (PARTITION BY Play_Id ORDER BY Date) AS Initial_Credit
	FROM 
		`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>` 
	WHERE 
		Adjusted_Underlying_Symbol IS NOT NULL 
	GROUP BY 
		Adjusted_Underlying_Symbol, 
		Date, 
		Play_Id
	ORDER BY 
		Adjusted_Underlying_Symbol, 
		Play_Id) AS B

ON

		A.Play_Id = B.Play_Id 
	AND 
		A.Adjusted_Underlying_Symbol = B.Adjusted_Underlying_Symbol

INNER JOIN

	(SELECT 
		Adjusted_Underlying_Symbol, 
		Play_Id, 
	  	
		STRING_AGG(
			CONCAT(
				SAFE_CAST(Strike_Price AS STRING), 
				IF(Call_or_Put = "CALL", "C", "P"), 
				" ", 
				IF(ABS(NBSF) > 1, CONCAT(" (" ,SAFE_CAST(NBSF AS STRING), ")"), "")), "  " 
	    ORDER BY 
	  		Adjusted_Underlying_Symbol, 
			Expiration_Date, 
			Call_or_Put DESC, 
			Strike_Price) AS Position,
	  	
		STRING_AGG(
		  	SAFE_CAST(FORMAT_DATE("%b-%d", Expiration_Date) AS STRING), "  " 
	  	ORDER BY 
			Adjusted_Underlying_Symbol, 
			Expiration_Date, 
			Call_or_Put DESC, 
			Strike_Price) AS Expirations
  
FROM 
	  (SELECT 
		  Adjusted_Underlying_Symbol, 
		  Symbol, 
		  Play_Id, 
		  Expiration_Date, 
		  Strike_Price, 
		  Call_or_Put, 
		  Notes, 
		  SUM(Buy_Sell_Flag) AS NBSF, 
		  SUM(Open_Close_Flag) AS Open
	  FROM 
	  	`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>`
	  GROUP BY 
	  	Adjusted_Underlying_Symbol, 
		Play_Id, Symbol, 
		Expiration_Date,
		Strike_Price, 
		Call_or_Put, 
		Notes
	  HAVING 
	  	SUM(Open_Close_Flag) > 0) 
	GROUP BY 
		Adjusted_Underlying_Symbol, 
		Play_Id) AS C

ON 

		A.Play_Id = C.Play_Id 
	AND 
		A.Adjusted_Underlying_Symbol = C.Adjusted_Underlying_Symbol

LEFT OUTER JOIN

	(SELECT 
		Adjusted_Underlying_Symbol, 
		Play_Id, 
		Invert
	
	FROM
	  
	  (SELECT 
		Adjusted_Underlying_Symbol, 
		Play_Id, 
		Expiration_Date, 
		Strike_Price, 
		Call_or_Put, 
		SUM(Buy_Sell_Flag) AS NBSF, 
		SUM(Open_Close_Flag) AS Open,
		Strike_Price - LEAD(Strike_Price, 1) OVER (PARTITION BY Play_Id ORDER BY Adjusted_Underlying_Symbol, Call_or_Put DESC, Strike_Price) as Invert
	  FROM 
	  	`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>`
	  GROUP BY 
	  	Adjusted_Underlying_Symbol, 
		Play_Id, 
		Symbol, 
		Expiration_Date,
		Strike_Price, 
		Call_or_Put
	  	HAVING SUM(Open_Close_Flag) > 0 AND SUM(Buy_Sell_Flag) < 0) -- end subquery
	
	WHERE 
		Invert > 0
	ORDER BY 
		Adjusted_Underlying_Symbol, 
		Call_or_Put DESC, 
		Strike_Price) AS D

ON

		A.Play_Id = D.Play_Id 
	AND 
		A.Adjusted_Underlying_Symbol = D.Adjusted_Underlying_Symbol

LEFT OUTER JOIN

	(SELECT 
		Adjusted_Underlying_Symbol, 
		Play_Id, 
		STRING_AGG(SAFE_CAST(Width AS STRING), " | " ORDER BY Adjusted_Underlying_Symbol, Play_Id) AS Spread
	FROM
		(SELECT 
			Adjusted_Underlying_Symbol, 
			Play_Id, 
			Expiration_Date, 
			Strike_Price , 
			Call_or_Put, 
			SUM(Buy_Sell_Flag) AS NBSF, 
			SUM(Open_Close_Flag) AS Open,
			Strike_Price - LAG(Strike_Price, 1) OVER (PARTITION BY Play_Id, Call_or_Put ORDER BY Adjusted_Underlying_Symbol, Strike_Price) as Width
		FROM 
			`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_HISTORY_TABLE_NAME>`
		GROUP BY 
			Adjusted_Underlying_Symbol, 
			Play_Id, 
			Symbol, 
			Expiration_Date,
			Strike_Price, 
			Call_or_Put
		HAVING 
			SUM(Open_Close_Flag) > 0)
	WHERE Width IS NOT NULL
	GROUP BY Adjusted_Underlying_Symbol, Play_Id
	ORDER BY Adjusted_Underlying_Symbol, Play_Id) AS E

ON

		A.Play_Id = E.Play_Id 
	AND 
		A.Adjusted_Underlying_Symbol = E.Adjusted_Underlying_Symbol

LEFT OUTER JOIN

	(SELECT 
		Play_Id, Notes 
	FROM 
		`<INSERT_PROJECT_ID>.<INSERT_DATASET_NAME>.<INSERT_PLAY_ID_NOTES_TABLE_NAME>`) AS F
	
ON

	A.Play_Id = F.Play_Id


ORDER BY A.Adjusted_Underlying_Symbol 



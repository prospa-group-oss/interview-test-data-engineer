SELECT c1.*
	,CASE 
		WHEN c1.C_acctbal < lowerQuartile
			THEN 'low'
		WHEN c1.C_acctbal < upperQuartile
			THEN 'mid'
		ELSE 'high'
		END AS bal_group
FROM customer c1
CROSS JOIN (
	SELECT min(C_acctbal)
		,max(C_acctbal)
		,((MIN(C_acctbal) + AVG(C_acctbal)) / 2) AS lowerQuartile
		,((MAX(C_acctbal) + AVG(C_acctbal)) / 2) AS upperQuartile
	FROM customer
	) c2
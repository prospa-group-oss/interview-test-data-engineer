SELECT nation.n_name
	,nation.N_comment
	,sum(lineitem.l_quantity * (1 - lineitem.l_discount) * lineitem.l_extendedprice) AS Total_Rev
FROM customer
INNER JOIN orders ON customer.c_custkey = orders.o_custkey
INNER JOIN lineitem ON lineitem.l_orderkey = orders.O_orderkey
LEFT JOIN nation ON nation.n_nationkey = customer.C_nationkey
GROUP BY nation.n_name
	,nation.N_comment
ORDER BY sum(lineitem.l_quantity * (1 - lineitem.l_discount) * lineitem.l_extendedprice) DESC limit 5
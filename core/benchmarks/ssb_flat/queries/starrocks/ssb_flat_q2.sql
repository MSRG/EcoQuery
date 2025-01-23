SELECT sum(lo_extendedprice * lo_discount) AS revenue FROM lineorder_flat
WHERE lo_orderdate >= '1994-01-01' and lo_orderdate <= '1994-01-31'
AND lo_discount BETWEEN 4 AND 6 AND lo_quantity BETWEEN 26 AND 35;
import duckdb

r1 = duckdb.query("SELECT 1 AS cage_code, 'a' AS val")
r2 = duckdb.query("SELECT 1 AS cage_code, 'b' AS other")

try:
    res = r1.join(r2, "cage_code", "left")
    print(res)
except Exception as e:
    print(e)

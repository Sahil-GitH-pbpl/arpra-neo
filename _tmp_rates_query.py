import pymysql
conn=pymysql.connect(host="localhost",user="root",password="",database="bhasin_7001",cursorclass=pymysql.cursors.DictCursor)
cur=conn.cursor()
company='Dr Khandelwal Clinic'
cur.execute("""
SELECT
    a.pname,
    pr.CompCatID,
    pr.GCode,
    pr.SCode,
    pr.TestCode,
    COALESCE(NULLIF(TRIM(pr.CTestCode), ''), t.Testcode1) AS BookedCode,
    COALESCE(NULLIF(TRIM(pr.CTestName), ''), t.Description) AS TestName,
    pr.Charge,
    pr.MRP,
    pr.MaxDiscount,
    pr.BookedFlag
FROM address a
JOIN PanelRates pr
  ON pr.CompCatID = a.category
LEFT JOIN test t
  ON t.GCode = pr.GCode
 AND t.SCode = pr.SCode
 AND t.TestCode = pr.TestCode
WHERE TRIM(a.pname) = %s
  AND pr.BookedFlag = 1
ORDER BY pr.GCode, pr.SCode, pr.TestCode
LIMIT 10
""", (company,))
for r in cur.fetchall():
    print(r)
cur.close();conn.close()

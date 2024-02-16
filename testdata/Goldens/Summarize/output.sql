WITH "__subquery0" AS (SELECT "Directory" AS "Directory", endsWith("FileName", '_test.go') AS "IsTest", sum("LineCount") AS "TotalLines" FROM "SourceFiles" GROUP BY "Directory", endsWith("FileName", '_test.go'))
SELECT * FROM "__subquery0" ORDER BY "Directory" ASC NULLS FIRST, "IsTest" ASC NULLS FIRST;

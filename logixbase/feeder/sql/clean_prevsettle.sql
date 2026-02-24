UPDATE [FUTURE_RESEARCH_1MIN].[dbo].ZC
SET [PrevSettle] = [PrevClose]
WHERE PK IN (SELECT PK
FROM  [FUTURE_RESEARCH_1MIN].[dbo].ZC
WHERE [PrevSettle] = 0 
OR [PrevSettle] / [PrevClose] > 1.2
OR [PrevSettle] / [PrevClose] < 0.8
)

SELECT * FROM
[FUTURE_RESEARCH_1MIN].[dbo].ZC
WHERE PK IN (
SELECT PK
FROM  [FUTURE_RESEARCH_1MIN].[dbo].ZC
WHERE [PrevSettle] = 0 
OR [PrevSettle] / [PrevClose] > 1.2
OR [PrevSettle] / [PrevClose] < 0.8
)
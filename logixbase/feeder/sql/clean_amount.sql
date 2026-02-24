
DECLARE @updateamount char(8000)
DECLARE @check char(8000)
DECLARE @table varchar(100)
SET @table = '[FUTURE_RESEARCH_DAILY].[dbo].[FUTUREQUOTE_DAILY]'

BEGIN
SET @updateamount= '
UPDATE '+@table+'
SET AMOUNT = AMOUNT_ 
FROM '+@table+' t1
	  INNER JOIN (SELECT a.[PK], ROUND(a.Amount * b.[Multiplier] / round(a.Amount / a.Volume / a.Settle, 0), 0) as Amount_
				  from  '+@table+' a
				  LEFT JOIN [FUTURE_RESEARCH_DAILY].[dbo].[FutureInfo_Basic] b
				  ON a.Ticker = b.Ticker
				  WHERE a.Volume != 0 
						AND a.Settle != 0 
						AND a.Amount != 0 
						AND a.[Settle] != 0 
						AND round(a.Amount / a.Volume  / a.[Settle], 0) != b.Multiplier
						AND round(a.Amount / a.Volume  / a.[Settle], 0) != 0) AS t2
on t1.[PK] = t2.[PK]
'
exec(@updateamount)

SET @check='
SELECT DISTINCT ROUND([AMOUNT] / [VOLUME] / [SETTLE], 0) AS MULTIPLIER
  FROM '+@table+'
  WHERE VOLUME != 0 AND AMOUNT != 0
  ORDER BY MULTIPLIER
'
exec(@check)

END

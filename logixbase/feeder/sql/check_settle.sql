SELECT * FROM  [FUTURE_RESEARCH_1MIN].[dbo].zc
WHERE [SETTLE] = 0 
OR [PrevSettle] = 0 
OR [Settle] / [Close] > 1.2
OR [Settle] / [Close] < 0.8
OR [PrevSettle] / [PrevClose] > 1.2
OR [PrevSettle] / [PrevClose] < 0.8
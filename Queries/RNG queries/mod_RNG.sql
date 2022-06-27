DECLARE @cnt INT = 0

DROP TABLE IF EXISTS #elapsed_time
CREATE TABLE #elapsed_time ( elapsed_ms BIGINT )

DECLARE
	@interval_start DATETIME2(3) = '2012-01-01 00:00:00.000',
	@interval_end DATETIME2(3) = '2019-01-01 00:00:00.000'

WHILE @cnt < 1
BEGIN
	DROP TABLE IF EXISTS #tmp
	CREATE TABLE #tmp ([type] VARCHAR(50), [key] VARCHAR(50), [range] VARCHAR(MAX))
	
	DECLARE @t1 DATETIME = GETDATE()
	
	DROP TABLE IF EXISTS #attribute
	SELECT a.[type], a.[key], eha.value
	INTO #attribute
	FROM event e
	JOIN event_has_attribute eha ON eha.event_id = e.id
	JOIN [attribute] a ON a.id = eha.attr_id 
	WHERE e.[time] >= @interval_start AND e.[time] < @interval_end
	GROUP BY a.[type] , a.[key] , eha.value
	
	
	INSERT INTO #tmp ( [type], [key], [range] )
	SELECT [type], [key], STRING_AGG(value, ', ')
	FROM #attribute
	WHERE [type] = 'literal' OR [type] = 'boolean'
	GROUP BY [type], [key] 
		
	INSERT INTO #tmp ( [type], [key], [range] )
	SELECT [type], [key], CONCAT( MIN(CONVERT(FLOAT,value)) , ' - ' , MAX(CONVERT(FLOAT,value)) )
	FROM #attribute
	WHERE [type] = 'continuous'
	GROUP BY [type], [key] 
	
	INSERT INTO #tmp ( [type], [key], [range] )
	SELECT [type], [key], CONCAT( MIN(CONVERT(BIGINT,value)) , ' - ' , MAX(CONVERT(BIGINT,value)) )
	FROM #attribute
	WHERE [type] = 'discrete'
	GROUP BY [type], [key] 
		
	INSERT INTO #tmp ( [type], [key], [range] )
	SELECT [type], [key], CONCAT( MIN(CONVERT(DATETIME2(3),value)) , ' - ' , MAX(CONVERT(DATETIME2(3),value)) )
	FROM #attribute
	WHERE [type] = 'timestamp'
	GROUP BY [type], [key] 
	
	DECLARE @t2 DATETIME = GETDATE()
	
	INSERT INTO #elapsed_time ( elapsed_ms )
	SELECT DATEDIFF(millisecond, @t1, @t2)
	
	SET @cnt = @cnt + 1
END

--SELECT * FROM #tmp
 
--DELETE TOP (2) FROM #elapsed_time
--SELECT AVG(elapsed_ms), STDEV(elapsed_ms) FROM #elapsed_time
SELECT * FROM #elapsed_time
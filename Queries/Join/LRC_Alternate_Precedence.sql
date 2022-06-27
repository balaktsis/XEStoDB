DECLARE @result TABLE ( Template VARCHAR(50), TaskA VARCHAR(300), TaskB VARCHAR(300), Support FLOAT );

DECLARE	@interval_start DATETIME2(3) = '0001-01-01 00:00:00.000',
		@interval_end DATETIME2(3) = '9999-12-31 23:59:59.999';

INSERT @result
SELECT	'AlternatePrecedence',
		TaskA,
		TaskB,
		(CAST(COUNT(*) AS FLOAT) / CAST( (SELECT COUNT(*) FROM @event WHERE task LIKE TaskB ) AS FLOAT) )
FROM (
	SELECT	a.trace_id,
			a.task AS TaskA,
			b.task AS TaskB
	FROM @event a 
	JOIN @event b ON ( a.log_id = b.log_id AND a.trace_id = b.trace_id 
						AND a.task != b.task AND a.[time] < b.[time] )
	WHERE a.[time] >= @interval_start AND a.[time] < @interval_end
		AND b.[time] >= @interval_start AND b.[time] < @interval_end
		AND NOT EXISTS (
			SELECT * FROM @event c
			WHERE c.log_id = b.log_id AND c.trace_id = b.trace_id
				AND c.task = b.task AND c.[time] > a.[time] AND c.[time] < b.[time]
		)
	GROUP BY a.trace_id, a.task, b.task, b.[time]
) discovery
GROUP BY TaskA, TaskB;
DECLARE @result TABLE ( Template VARCHAR(50), TaskA VARCHAR(300), TaskB VARCHAR(300), Support FLOAT );

DECLARE	@interval_start DATETIME2(3) = '0001-01-01 00:00:00.000',
		@interval_end DATETIME2(3) = '9999-12-31 23:59:59.999';

INSERT @result
SELECT	'RespondedExistence',
		TaskA,
		TaskB,
		(CAST(COUNT(*) AS FLOAT) / CAST( (SELECT COUNT(*) FROM @event WHERE task LIKE TaskA ) AS FLOAT) )
FROM (
	SELECT	a.trace_id,
			a.task AS TaskA,
			b.task AS TaskB
	FROM @event a 
	JOIN @event b ON ( a.log_id = b.log_id AND a.trace_id = b.trace_id 
						AND a.task != b.task )
	WHERE a.[time] >= @interval_start AND a.[time] < @interval_end
		AND b.[time] >= @interval_start AND b.[time] < @interval_end
	GROUP BY a.trace_id, a.task, a.[time], b.task
) discovery
GROUP BY TaskA, TaskB;
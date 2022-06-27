DECLARE @result TABLE ( Template VARCHAR(50), TaskA VARCHAR(300), TaskB VARCHAR(300), Support FLOAT );

INSERT @result
SELECT	'ChainResponse',
		TaskA,
		TaskB,
		(CAST(COUNT(*) AS FLOAT) / CAST( (SELECT COUNT(*) FROM @event WHERE task LIKE TaskA ) AS FLOAT) )
FROM (
	SELECT	a.trace_id,
			a.task AS TaskA,
			b.task AS TaskB
	FROM @event a 
	JOIN @event b ON ( a.log_id = b.log_id AND a.trace_id = b.trace_id 
						AND a.task != b.task AND a.[time] < b.[time] )
	WHERE NOT EXISTS (
		SELECT * FROM @event c 
		WHERE c.log_id = a.log_id AND c.trace_id = a.trace_id
			AND c.[time] > a.[time] AND c.[time] < b.[time]
		)
	GROUP BY a.trace_id, a.task, a.[time], b.task
) discovery
GROUP BY TaskA, TaskB;
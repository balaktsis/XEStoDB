DECLARE @result TABLE ( Template VARCHAR(50), TaskA VARCHAR(300), TaskB VARCHAR(300), Support FLOAT );

INSERT @result
SELECT	'Response',
		TaskA,
		TaskB,
		(CAST(COUNT(*) AS FLOAT) / CAST( (SELECT COUNT(*) FROM @event WHERE task LIKE TaskA ) AS FLOAT) )
FROM @event a, (
	SELECT a.task AS TaskA, b.task AS TaskB 
	FROM @event a, @event b 
	WHERE a.task != b.task 
	GROUP BY a.task, b.task
) x
WHERE a.task = x.TaskA 
	AND EXISTS (
		SELECT * FROM @event b 
		WHERE b.task = x.TaskB AND a.log_id = b.log_id AND a.trace_id = b.trace_id AND b.[time] > a.[time] 
	)
GROUP BY x.TaskA , x.TaskB;
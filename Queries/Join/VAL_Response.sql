DECLARE @discovery TABLE ( trace_id BIGINT, TaskA VARCHAR(300), TimeA DATETIME2(3), TaskB VARCHAR(300), TimeB DATETIME2(3) );

INSERT @discovery
SELECT	a.trace_id,
		a.task,
		a.[time],
		b.task,
		MIN(b.[time])
FROM @event a 
JOIN @event b ON ( a.log_id = b.log_id AND a.trace_id = b.trace_id 
					AND a.task != b.task AND a.[time] < b.[time] )
GROUP BY a.trace_id, a.task, a.[time], b.task;

DECLARE @result TABLE ( Template VARCHAR(50), trace_id BIGINT, TaskA VARCHAR(300), TaskB VARCHAR(300), VAL_start DATETIME2(3), VAL_end DATETIME2(3) );

INSERT @result
SELECT	'Response',
		trace_id,
		TaskA,
		TaskB,
		MIN(TimeA),
		MAX(TimeB)
FROM (
	SELECT trace_id , TaskA , TimeA , TaskB , TimeB, MIN(next_violation_time) AS next_violation_time
	FROM (
		SELECT	fulfillment.*,
				(	SELECT MIN(violation.TimeA)
					FROM (SELECT * FROM @discovery WHERE TimeB IS NULL) violation
					WHERE violation.trace_id = fulfillment.trace_id
						AND violation.TaskA = fulfillment.TaskA
						AND violation.TaskB = fulfillment.TaskB
						AND violation.TimeA > fulfillment.TimeA
				) AS next_violation_time
		FROM (SELECT * FROM @discovery WHERE TimeB IS NOT NULL) fulfillment
	) subq1
	GROUP BY trace_id , TaskA , TimeA , TaskB , TimeB
) subq2
GROUP BY trace_id , TaskA , TaskB , next_violation_time;
DECLARE @time_dist TABLE ( trace_id BIGINT, TaskA VARCHAR(300), TimeA DATETIME2(3), TaskB VARCHAR(300), TimeB DATETIME2(3), time_dist BIGINT );

INSERT @time_dist
SELECT	a.trace_id,
		a.task, 
		a.[time],
		b.task,
		b.[time],
		ABS(DATEDIFF_BIG(SECOND, a.[time], b.[time]))
FROM @event a 
JOIN @event b ON ( a.log_id = b.log_id AND a.trace_id = b.trace_id 
					AND a.task != b.task );

DECLARE @discovery TABLE ( trace_id BIGINT, TaskA VARCHAR(300), TimeA DATETIME2(3), TaskB VARCHAR(300), TimeB DATETIME2(3) );

INSERT @discovery
SELECT	trace_id,
		TaskA,
		TimeA,
		TaskB,
		MAX(TimeB)
FROM @time_dist td1
WHERE time_dist = (
	SELECT MIN(time_dist) FROM @time_dist td2
	WHERE td1.trace_id = td2.trace_id AND td1.TaskA = td2.TaskA 
		AND td1.TimeA = td2.TimeA AND td1.TaskB = td2.TaskB 
	)
GROUP BY trace_id , TaskA , TimeA , TaskB;

DECLARE @result TABLE ( Template VARCHAR(50), trace_id BIGINT, TaskA VARCHAR(300), TaskB VARCHAR(300), VAL_start DATETIME2(3), VAL_end DATETIME2(3) );

INSERT @result
SELECT	'Response',
		trace_id,
		TaskA,
		TaskB,
		MIN(VAL_start),
		MAX(VAL_end)
FROM (
	SELECT trace_id , TaskA , IIF(TimeA<TimeB, TimeA, TimeB) AS VAL_start , TaskB , IIF(TimeA<TimeB, TimeB, TimeA) AS VAL_end, MIN(next_violation_time) AS next_violation_time
	FROM (
		SELECT	fulfillment.*,
				(	SELECT MIN(violation.TimeA)
					FROM (SELECT * FROM @discovery WHERE TimeB IS NULL) violation
					WHERE violation.trace_id = fulfillment.trace_id
						AND violation.TaskA = fulfillment.TaskA
						AND violation.TaskB = fulfillment.TaskB
						AND violation.TimeA > fulfillment.TimeA
						AND violation.TimeA > fulfillment.TimeB
				) AS next_violation_time
		FROM (SELECT * FROM @discovery WHERE TimeB IS NOT NULL) fulfillment
	) subq1
	GROUP BY trace_id , TaskA , TimeA , TaskB , TimeB
) subq2
GROUP BY trace_id , TaskA , TaskB , next_violation_time;
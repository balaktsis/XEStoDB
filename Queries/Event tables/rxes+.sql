DECLARE @event TABLE ( log_id BIGINT, trace_id BIGINT, task VARCHAR(300), [time] DATETIME2(3) );

INSERT @event
SELECT	t.log_id,
		e.trace_id,
		e.name + '_' + e.transition,
		e.[time]
FROM trace t 
JOIN event e ON t.id = e.trace_id;
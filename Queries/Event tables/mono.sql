DECLARE @event TABLE ( log_id BIGINT, trace_id BIGINT, task VARCHAR(300), [time] DATETIME2(3) );

INSERT @event
SELECT	log_id,
		trace_id,
		event_name + '_' + event_transition,
		event_timestamp
FROM log;
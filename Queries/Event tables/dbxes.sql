DECLARE @event TABLE ( log_id BIGINT, trace_id BIGINT, task VARCHAR(300), [time] DATETIME2(3) );

INSERT @event
SELECT	lht.log_id,
		the.trace_id,
		(SELECT value FROM attribute WHERE id = eha1.attr_id) + '_' + (SELECT value FROM attribute WHERE id = eha2.attr_id),
		CAST( (SELECT value FROM attribute WHERE id = eha3.attr_id) AS DATETIME2(3))
FROM log_has_trace lht 
JOIN trace_has_event the ON lht.trace_id = the.trace_id
JOIN event_has_attribute eha1 ON the.event_id = eha1.event_id  
JOIN event_has_attribute eha2 ON eha1.event_id = eha2.event_id
JOIN event_has_attribute eha3 ON eha1.event_id = eha3.event_id
WHERE eha1.attr_id IN (SELECT id FROM attribute WHERE [key] = 'concept:name')
	AND eha2.attr_id IN (SELECT id FROM attribute WHERE [key] = 'lifecycle:transition')
	AND eha3.attr_id IN (SELECT id FROM attribute WHERE [key] = 'time:timestamp');
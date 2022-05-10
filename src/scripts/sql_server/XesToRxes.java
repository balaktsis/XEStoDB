package scripts.sql_server;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;

import commons.Commons;

public class XesToRxes {

	private static final String USER = "sa";
	private static final String PWD = "Riva96_shared_db";
	private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	
	private enum Scope {NONE, EVENT, TRACE};
	
	private static long dupEvents = 0;
	private static long dupTraces = 0;
	
	public static void main(String[] args) throws IOException {
		System.out.print("Enter name of the database to populate: ");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String dbName = "rxes_" + reader.readLine();
    	String dbUrl = "jdbc:sqlserver://localhost:1433;databaseName=" + dbName + ";encrypt=true;trustServerCertificate=true;";
    	
		File logFile = Commons.selectLogFile();	//new File(System.getProperty("user.dir"), "prova.xes");
		if (logFile == null) return;
		
		System.out.println("Parsing XES file ... ");
		List<XLog> list = Commons.convertToXlog(logFile);
		System.out.println("Complete!");
		
		try (Connection conn = Commons.getConnection(USER, PWD, dbUrl, DRIVER_CLASS)) {
			
			List<Long> evtInsTimes = new LinkedList<>();
			long startTime = System.currentTimeMillis();
			
			try (Statement st = conn.createStatement()) {
				// Clearing all data previously contained in the database
				st.execute("EXEC sp_MSForEachTable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL';");
				st.execute("EXEC sp_MSForEachTable 'DELETE FROM ?';");
				st.execute("EXEC sp_MSForEachTable 'ALTER TABLE ? CHECK CONSTRAINT ALL';");
				
				long logCtr, traceCtr, eventCtr;
				ResultSet resultingId;
				
				for (XLog log : list) {
					resultingId = st.executeQuery("SELECT COALESCE( (SELECT MAX(id) FROM log)+1 , 0 );");
					resultingId.next();
					logCtr = resultingId.getLong(1);
					
					System.out.println("Log " + (logCtr+1) + " - Converting extensions");
					insertExtensions(st, log.getExtensions());
					
					List<Long> traceIdSequence = new ArrayList<>();
					
					for (XTrace trace : log) {
						resultingId = st.executeQuery("SELECT COALESCE( (SELECT MAX(id) FROM trace)+1 , 0 );");
						resultingId.next();
						traceCtr = resultingId.getLong(1);
						
						System.out.println("Log " + (logCtr+1) + " - Converting trace " + (traceCtr+1) + " of " + log.size());
						List<Long> newEventIdSequence = new ArrayList<>();
						
						for (XEvent event : trace) {
							long evtInsStart = System.currentTimeMillis();
							
							resultingId = st.executeQuery("SELECT COALESCE( (SELECT MAX(id) FROM event)+1 , 0 );");
							resultingId.next();
							eventCtr = resultingId.getLong(1);
							
							Map<Long, String> newEvtAttIdsAndVals = insertAttributes(st, event.getAttributes().values(), null);
							
							try (Statement resultSetStmt = conn.createStatement()) {
								ResultSet existingIds = resultSetStmt.executeQuery("SELECT id FROM event");
								
								if (!existingIds.next()) {	// Empty event table
									insertEvent(st, eventCtr, null, newEvtAttIdsAndVals);									
									newEventIdSequence.add(eventCtr);
									
								} else {
									boolean isDuplicate = false;
									
									do {
										long eventId = existingIds.getLong(1);
										
										Map<Long, String> evtAttIdsAndVals = selectAttributesIdsAndValuesFromEha(st, eventId);
										
										// Equality should be checked also over event collections
										if (newEvtAttIdsAndVals.equals(evtAttIdsAndVals)) {
											newEventIdSequence.add(eventId);
											dupEvents++;
											isDuplicate = true;
										}
										
									} while (existingIds.next() && !isDuplicate);
									
									if (!isDuplicate) {
										insertEvent(st, eventCtr, null, newEvtAttIdsAndVals);
										newEventIdSequence.add(eventCtr);
									}
								}
							}
							
							long evtInsEnd = System.currentTimeMillis();
							evtInsTimes.add(evtInsEnd-evtInsStart);
						}
						
						Map<Long, String> newTrcAttIdsAndVals = insertAttributes(st, trace.getAttributes().values(), null);
						
						try (Statement resultSetStmt = conn.createStatement()) {
							ResultSet existingIds = resultSetStmt.executeQuery("SELECT id FROM trace");
							
							if (!existingIds.next()) {	// Empty trace table
								insertTrace(st, traceCtr, newEventIdSequence, newTrcAttIdsAndVals);
								traceIdSequence.add(traceCtr);
								
							} else {
								boolean isDuplicate = false;
								
								do {
									long traceId = existingIds.getLong(1);
									
									Map<Long, String> trcAttIdsAndVals = selectAttributesIdsAndValuesFromTha(st, traceId);
									
									// If the new trace has same attributes and event sequence of an existent one, then it is a duplicate
									if (newTrcAttIdsAndVals.equals(trcAttIdsAndVals)) {
										List<Long> eventIdSequence = selectIdSequenceFromThe(st, traceId);
										
										if (newEventIdSequence.equals(eventIdSequence)) {
											traceIdSequence.add(traceId);
											dupTraces++;
											isDuplicate = true;
										}
									}
									
								} while (existingIds.next() && !isDuplicate);
								
								if (!isDuplicate) {
									insertTrace(st, traceCtr, newEventIdSequence, newTrcAttIdsAndVals);
									traceIdSequence.add(traceCtr);
								}
							}
						}
					}
					
					String logName = XConceptExtension.instance().extractName(log);
					insertLog(st, logCtr, logName, traceIdSequence);
					
					System.out.println("Log " + (logCtr+1) + " - Converting log attributes");
					insertLogAttributes(st, logCtr, log.getAttributes().values().stream().filter(att -> !att.getKey().equals(XConceptExtension.KEY_NAME)).collect(Collectors.toList()), null, Scope.NONE);
					insertLogAttributes(st, logCtr, log.getGlobalTraceAttributes(), null, Scope.TRACE);
					insertLogAttributes(st, logCtr, log.getGlobalEventAttributes(), null, Scope.EVENT);
					
					System.out.println("Log " + (logCtr+1) + " - Converting classifiers");
					insertLogClassifiers(st, logCtr, log.getClassifiers());
				}
			}
		
			long endTime = System.currentTimeMillis();
			double elapsedTime = ((double) (endTime-startTime)) / 1000;
			System.out.println("Succesfully concluded in " + elapsedTime + " seconds");
			System.out.println("DupEvents = " + dupEvents);
			System.out.println("DupTraces = " + dupTraces);
			
			System.out.print("Writing event insertion times to file... ");
			File csvOutputFile = new File("rxes_eventInsertionTimes.csv");
			try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
				pw.print( evtInsTimes.stream().map(l -> String.valueOf(l)).collect(Collectors.joining(",")) );
				System.out.println("Complete!");
			}
			
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static void insertExtensions(Statement stmt, Set<XExtension> extSet) throws SQLException {
		for (XExtension ext : extSet) {
			String name = Commons.prepareValueForInsertion(ext.getName(), 50);
			String prefix = Commons.prepareValueForInsertion(ext.getPrefix(), 50);
			String uri = Commons.prepareValueForInsertion(ext.getUri().toString(), 250);
			
			stmt.execute(
				"IF NOT EXISTS (SELECT * FROM extension "
								+ "WHERE name" + Commons.selectPredicate(name) + name
								+ " AND prefix" + Commons.selectPredicate(prefix) + prefix
								+ " AND uri" + Commons.selectPredicate(uri) + uri
							+ ") "
					+ "INSERT INTO extension ( id, name, prefix, uri ) "
					+ "VALUES ( COALESCE( (SELECT MAX(id) FROM extension)+1 , 0), " + name + ", " + prefix + ", " + uri + " );"
			);
		}
	}
	
	private static Map<Long,String> insertAttributes(Statement stmt, Collection<XAttribute> attributes, Long parentId) throws SQLException {
		Map<Long,String> idsAndVals = new HashMap<>();
		
		for (XAttribute att : attributes) {
			String key = Commons.prepareValueForInsertion(att.getKey(), 50);
			
			String type;
			if (att instanceof XAttributeBoolean)
				type = "'boolean'";
			else if (att instanceof XAttributeContinuous)
				type = "'continuous'";
			else if (att instanceof XAttributeDiscrete)
				type = "'discrete'";
			else if (att instanceof XAttributeTimestamp)
				type = "'timestamp'";
			else	// Treating all other types as literals
				type = "'literal'";
			
			String extIdStr;
			if (att.getExtension() != null) {
				String extName = Commons.prepareValueForInsertion(att.getExtension().getName(), 50);
				String extPrefix = Commons.prepareValueForInsertion(att.getExtension().getPrefix(), 50);
				String extUri = Commons.prepareValueForInsertion(att.getExtension().getUri().toString(), 250);
				
				ResultSet extId = stmt.executeQuery(
					"SELECT id FROM extension "
					+ "WHERE name" + Commons.selectPredicate(extName) + extName
						+ " AND prefix" + Commons.selectPredicate(extPrefix) + extPrefix
						+ " AND uri" + Commons.selectPredicate(extUri) + extUri + ";"
				);
				
				extId.next();
				extIdStr = extId.getString(1);
			} else {
				extIdStr = "NULL";
			}
			
			String parentIdStr = parentId == null ? "NULL" : String.valueOf(parentId);
			
			// Returns the attribute id inside the DB if it is already there, else inserts the attribute and returns the newly added id
			ResultSet attributeId = stmt.executeQuery(
				"DECLARE @att_id BIGINT = NULL, "
						+ "@next_id BIGINT = COALESCE( (SELECT MAX(id) FROM attribute)+1 , 0 );"
						
				+ "SELECT TOP(1) @att_id = id FROM attribute "
				+ "WHERE [type]" + Commons.selectPredicate(type) + type
					+ " AND [key]" + Commons.selectPredicate(key) + key
					+ " AND ext_id" + Commons.selectPredicate(extIdStr) + extIdStr
					+ " AND parent_id" + Commons.selectPredicate(parentIdStr) + parentIdStr + ";"
				
				
				+ "IF @att_id IS NULL "
					+ "INSERT INTO attribute ( id, [type], [key], ext_id, parent_id ) "
					+ "OUTPUT INSERTED.id "
					+ "VALUES ( @next_id, " + type + ", " + key + ", " + extIdStr + ", " + parentIdStr + " ) "
				+ "ELSE "
					+ "SELECT @att_id;"
			);
			
			attributeId.next();
			idsAndVals.put(attributeId.getLong(1), att.toString());
			
			// Recursion over nested attributes
			idsAndVals.putAll( insertAttributes(stmt, att.getAttributes().values(), attributeId.getLong(1)) );
		}
		
		return idsAndVals;
	}
	
	private static Map<Long, String> selectAttributesIdsAndValuesFromEha(Statement stmt, long eventId) throws SQLException {
		Map<Long, String> idsAndVals = new HashMap<>();
				
		ResultSet idAndVal = stmt.executeQuery(
			"SELECT attr_id, value FROM event_has_attribute "
			+ "WHERE event_id = " + eventId
		);
		
		while (idAndVal.next())
			idsAndVals.put(idAndVal.getLong(1), idAndVal.getString(2));
		
		return idsAndVals;
	}
	
	private static Map<Long, String> selectAttributesIdsAndValuesFromTha(Statement stmt, long traceId) throws SQLException {
		Map<Long, String> idsAndVals = new HashMap<>();
		
		ResultSet idAndVal = stmt.executeQuery(
			"SELECT attr_id, value FROM trace_has_attribute "
			+ "WHERE trace_id = " + traceId
		);
		
		while (idAndVal.next())
			idsAndVals.put(idAndVal.getLong(1), idAndVal.getString(2));
		
		return idsAndVals;
	}
	
	private static List<Long> selectIdSequenceFromThe(Statement stmt, long traceId) throws SQLException {
		List<Long> eventIdSequence = new ArrayList<>();
		
		ResultSet eventId = stmt.executeQuery(
			"SELECT event_id FROM trace_has_event "
			+ "WHERE trace_id = " + traceId
		);
		
		while (eventId.next())
			eventIdSequence.add(eventId.getLong(1));
		
		return eventIdSequence;
	}
	
	private static void insertEvent(Statement stmt, long eventId, Long eventCollId, Map<Long,String> attIdsAndVals) throws SQLException {
		String eventCollIdStr = eventCollId == null ? "NULL" : eventCollId.toString();
		
		stmt.execute(
			"INSERT INTO event ( id, event_coll_id ) "
			+ "VALUES ( " + eventId + ", " + eventCollIdStr + " );"
		);
		
		for (Map.Entry<Long, String> entry : attIdsAndVals.entrySet()) {
			long attributeId = entry.getKey();
			String value = Commons.prepareValueForInsertion(entry.getValue(), 250);
			
			stmt.execute(
				"INSERT INTO event_has_attribute ( event_id, attr_id, value ) "
				+ "VALUES ( " + eventId + ", " + attributeId + ", " + value + " );"
			);
		}
	}
	
	private static void insertTrace(Statement stmt, long traceId, List<Long> eventIdSequence, Map<Long,String> attIdsAndVals) throws SQLException {
		
		stmt.execute(
			"INSERT INTO trace ( id ) "
			+ "VALUES ( " + traceId + " );"
		);
		
		for (long eventId : eventIdSequence) {
			stmt.execute(
				"INSERT INTO trace_has_event ( sequence, trace_id, event_id ) "
				+ "VALUES ( (SELECT COUNT(*) FROM trace_has_event), " + traceId + ", " + eventId + " );"
			);
		}
		
		for (Map.Entry<Long, String> entry : attIdsAndVals.entrySet()) {
			long attributeId = entry.getKey();
			String value = Commons.prepareValueForInsertion(entry.getValue(), 250);
			
			stmt.execute(
				"INSERT INTO trace_has_attribute ( trace_id, attr_id, value ) "
				+ "VALUES ( " + traceId + ", " + attributeId + ", " + value + " );"
			);
		}
	}

	private static void insertLog(Statement stmt, long logId, String logName, List<Long> traceIdSequence) throws SQLException {
		
		stmt.execute(
			"INSERT INTO log ( id, name ) "
			+ "VALUES ( " + logId + ", " + Commons.prepareValueForInsertion(logName, 250) + " );"
		);
		
		for (long traceId : traceIdSequence) {
			
			stmt.execute(
				"INSERT INTO log_has_trace ( sequence, log_id, trace_id ) "
				+ "VALUES ( (SELECT COUNT(*) FROM log_has_trace), " + logId + ", " + traceId + " );"
			);
		}
	}
	
	private static void insertLogAttributes(Statement stmt, long logId, Collection<XAttribute> attributes, Long parentId, Scope scope) throws SQLException {
		for (XAttribute att : attributes) {
			String key = Commons.prepareValueForInsertion(att.getKey(), 50);
			
			String type;
			if (att instanceof XAttributeBoolean)
				type = "'boolean'";
			else if (att instanceof XAttributeContinuous)
				type = "'continuous'";
			else if (att instanceof XAttributeDiscrete)
				type = "'discrete'";
			else if (att instanceof XAttributeTimestamp)
				type = "'timestamp'";
			else	// Treating all other types as literals
				type = "'literal'";
			
			String extIdStr;
			if (att.getExtension() != null) {
				String extName = Commons.prepareValueForInsertion(att.getExtension().getName(), 50);
				String extPrefix = Commons.prepareValueForInsertion(att.getExtension().getPrefix(), 50);
				String extUri = Commons.prepareValueForInsertion(att.getExtension().getUri().toString(), 250);
				
				ResultSet extId = stmt.executeQuery(
					"SELECT id FROM extension "
					+ "WHERE name" + Commons.selectPredicate(extName) + extName
						+ " AND prefix" + Commons.selectPredicate(extPrefix) + extPrefix
						+ " AND uri" + Commons.selectPredicate(extUri) + extUri + ";"
				);
				
				extId.next();
				extIdStr = extId.getString(1);
			} else {
				extIdStr = "NULL";
			}
			
			String parentIdStr = parentId == null ? "NULL" : String.valueOf(parentId);
			
			// Returns the attribute id inside the DB if it is already there, else inserts the attribute and returns the newly added id
			ResultSet attributeIdQuery = stmt.executeQuery(
				"DECLARE @att_id BIGINT = NULL, "
						+ "@next_id BIGINT = COALESCE( (SELECT MAX(id) FROM attribute)+1 , 0 );"
						
				+ "SELECT TOP(1) @att_id = id FROM attribute "
				+ "WHERE [type]" + Commons.selectPredicate(type) + type
				+ " AND [key]" + Commons.selectPredicate(key) + key
					+ " AND ext_id" + Commons.selectPredicate(extIdStr) + extIdStr
					+ " AND parent_id" + Commons.selectPredicate(parentIdStr) + parentIdStr + ";"
				
				
				+ "IF @att_id IS NULL "
					+ "INSERT INTO attribute ( id, [type], [key], ext_id, parent_id ) "
					+ "OUTPUT INSERTED.id "
					+ "VALUES ( @next_id, " + type + ", " + key + ", " + extIdStr + ", " + parentIdStr + " ) "
				+ "ELSE "
					+ "SELECT @att_id;"
			);
			
			attributeIdQuery.next();
			long attributeId = attributeIdQuery.getLong(1);
						
			String traceGlobal = "'FALSE'";
			String eventGlobal = "'FALSE'";
			switch (scope) {
			case TRACE:
				traceGlobal = "'TRUE'";
				break;
			case EVENT:
				eventGlobal = "'TRUE'";
				break;
			}
			
			String value = Commons.prepareValueForInsertion(att.toString(), 250);
			
			stmt.execute(
				"IF NOT EXISTS (SELECT 1 FROM log_has_attribute "
								+ "WHERE log_id = " + logId
								+ " AND trace_global = " + traceGlobal
								+ " AND event_global = " + eventGlobal
								+ " AND attr_id = " + attributeId
							+ ") "
					+ "INSERT INTO log_has_attribute ( log_id, trace_global, event_global, attr_id, value )"
					+ "VALUES ( " + logId + ", " + traceGlobal + ", " + eventGlobal + ", " + attributeId + ", " + value + " );"
			);
			
			// Recursion over nested attributes
			insertLogAttributes(stmt, logId, att.getAttributes().values(), attributeId, scope);
		}
	}

	private static void insertLogClassifiers(Statement stmt, long logId, List<XEventClassifier> classifierList) throws SQLException {
		
		for (XEventClassifier classif : classifierList) {
			String name = Commons.prepareValueForInsertion(classif.name(), 50);
			String keys = Commons.prepareValueForInsertion(String.join(", ", classif.getDefiningAttributeKeys()), 250);
			
			stmt.execute(
				"IF NOT EXISTS (SELECT * FROM classifier "
								+ "WHERE name" + Commons.selectPredicate(name) + name
								+ " AND keys" + Commons.selectPredicate(keys) + keys
								+ " AND log_id = " + logId
							+ ") "
					+ "INSERT INTO classifier ( id, name, keys, log_id ) "
					+ "VALUES ( COALESCE( (SELECT MAX(id) FROM classifier)+1 , 0)" + ", " + name + ", " + keys + ", " + logId + " );"
			);
		}
	}
}

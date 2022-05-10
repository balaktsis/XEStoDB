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
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.extension.std.XLifecycleExtension;
import org.deckfour.xes.extension.std.XTimeExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;

import commons.Commons;

public class XesToRxesIndexed {
	
	private static final String USER = "sa";
	private static final String PWD = "Riva96_shared_db";
	private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	
	private enum Scope {NONE, EVENT, TRACE};
	
	public static void main(String[] args) throws IOException {
		System.out.print("Enter name of the database to populate: ");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String dbName = "ind_" + reader.readLine();
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
					
					String logName = XConceptExtension.instance().extractName(log);
					insertLog(st, logCtr, logName);
					
					System.out.println("Log " + (logCtr+1) + " - Converting extensions");
					insertExtensions(st, log.getExtensions());
					
					System.out.println("Log " + (logCtr+1) + " - Converting classifiers");
					insertLogClassifiers(st, logCtr, log.getClassifiers());
					
					for (XTrace trace : log) {
						resultingId = st.executeQuery("SELECT COALESCE( (SELECT MAX(id) FROM trace)+1 , 0 );");
						resultingId.next();
						traceCtr = resultingId.getLong(1);
						
						System.out.println("Log " + (logCtr+1) + " - Converting trace " + (traceCtr+1) + " of " + log.size());
						
						String traceName = XConceptExtension.instance().extractName(trace);
						insertTrace(st, traceCtr, traceName, logCtr);
						insertTraceAttributes(st, traceCtr, trace.getAttributes().values().stream().filter(att -> !att.getKey().equals(XConceptExtension.KEY_NAME)).collect(Collectors.toList()), null);
						
						for (XEvent event : trace) {
							long evtInsStart = System.currentTimeMillis();
							
							resultingId = st.executeQuery("SELECT COALESCE( (SELECT MAX(id) FROM event)+1 , 0 );");
							resultingId.next();
							eventCtr = resultingId.getLong(1);
							
							String eventName = XConceptExtension.instance().extractName(event);
							String eventTransition = XLifecycleExtension.instance().extractTransition(event);
							Date eventTimestamp = XTimeExtension.instance().extractTimestamp(event);
							
							insertEvent(st, eventCtr, eventName, eventTransition, eventTimestamp, traceCtr);
							insertEventAttributes(
								st, 
								eventCtr, 
								event.getAttributes().values().stream()
																.filter(att -> !att.getKey().equals(XConceptExtension.KEY_NAME)
																		&& !att.getKey().equals(XLifecycleExtension.KEY_TRANSITION)
																		&& !att.getKey().equals(XTimeExtension.KEY_TIMESTAMP))
																.collect(Collectors.toList()), 
								null
							);
							
							long evtInsEnd = System.currentTimeMillis();
							evtInsTimes.add(evtInsEnd-evtInsStart);
						}
					}
					
					System.out.println("Log " + (logCtr+1) + " - Converting log attributes");
					insertLogAttributes(st, logCtr, log.getAttributes().values().stream().filter(att -> !att.getKey().equals(XConceptExtension.KEY_NAME)).collect(Collectors.toList()), null, Scope.NONE);
					insertLogAttributes(st, logCtr, log.getGlobalTraceAttributes(), null, Scope.TRACE);
					insertLogAttributes(st, logCtr, log.getGlobalEventAttributes(), null, Scope.EVENT);
				}
			}
			
			long endTime = System.currentTimeMillis();
			double elapsedTime = ((double) (endTime-startTime)) / 1000;
			System.out.println("Succesfully concluded in " + elapsedTime + " seconds");
			
			System.out.print("Writing event insertion times to file... ");
			File csvOutputFile = new File("ind_eventInsertionTimes.csv");
			try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
				pw.print( evtInsTimes.stream().map(l -> String.valueOf(l)).collect(Collectors.joining(",")) );
				System.out.println("Complete!");
			}
			
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static void insertLog(Statement stmt, long logId, String logName) throws SQLException {		
		stmt.execute(
			"INSERT INTO log ( id, name ) "
			+ "VALUES ( " + logId + ", " + Commons.prepareValueForInsertion(logName, 250) + " );"
		);
	}
	
	private static void insertExtensions(Statement stmt, Set<XExtension> extSet) throws SQLException {
		for (XExtension ext : extSet) {
			String name = Commons.prepareValueForInsertion(ext.getName(), 50);
			String prefix = Commons.prepareValueForInsertion(ext.getPrefix(), 50);
			String uri = Commons.prepareValueForInsertion(ext.getUri().toString(), 250);
			
			stmt.execute(
				"INSERT INTO extension ( id, name, prefix, uri ) "
				+ "VALUES ( COALESCE( (SELECT MAX(id) FROM extension)+1 , 0), " + name + ", " + prefix + ", " + uri + " );"
			);
		}
	}
	
	private static void insertLogClassifiers(Statement stmt, long logId, List<XEventClassifier> classifierList) throws SQLException {		
		for (XEventClassifier classif : classifierList) {
			String name = Commons.prepareValueForInsertion(classif.name(), 50);
			String keys = Commons.prepareValueForInsertion(String.join(", ", classif.getDefiningAttributeKeys()), 250);
			
			stmt.execute(
				"INSERT INTO classifier ( id, name, keys, log_id ) "
				+ "VALUES ( COALESCE( (SELECT MAX(id) FROM classifier)+1 , 0), " + name + ", " + keys + ", " + logId + " );"
			);
		}
	}
	
	private static void insertLogAttributes(Statement stmt, long logId, Collection<XAttribute> attributes, Long parentId, Scope scope) throws SQLException {
		
		for (XAttribute att : attributes) {
			long attributeId = insertAttribute(stmt, att, parentId);
						
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
				"INSERT INTO log_has_attribute ( log_id, trace_global, event_global, attr_id, value )"
				+ "VALUES ( " + logId + ", " + traceGlobal + ", " + eventGlobal + ", " + attributeId + ", " + value + " );"
			);
			
			// Recursion over nested attributes
			insertLogAttributes(stmt, logId, att.getAttributes().values(), attributeId, scope);
		}
	}

	private static void insertTrace(Statement stmt, long traceId, String traceName, long logId) throws SQLException {
		traceName = Commons.prepareValueForInsertion(traceName, 250);
		
		stmt.execute(
			"INSERT INTO trace ( id, name, log_id ) "
			+ "VALUES ( " + traceId + ", " + traceName + ", " + logId + " );"
		);
	}
	
	private static void insertTraceAttributes(Statement stmt, long traceId, Collection<XAttribute> attributes, Long parentId) throws SQLException {
		
		for (XAttribute att : attributes) {
			long attributeId = insertAttribute(stmt, att, parentId);
						
			String value = Commons.prepareValueForInsertion(att.toString(), 250);
			
			stmt.execute(
				"INSERT INTO trace_has_attribute ( trace_id, attr_id, value )"
				+ "VALUES ( " + traceId + ", " + attributeId + ", " + value + " );"
			);
			
			// Recursion over nested attributes
			insertTraceAttributes(stmt, traceId, att.getAttributes().values(), attributeId);
		}
	}
	
	private static void insertEvent(Statement stmt, long eventId, String eventName, String eventTransition, Date timestamp, long traceId) throws SQLException {
		eventName = Commons.prepareValueForInsertion(eventName, 250);
		eventTransition = Commons.prepareValueForInsertion(eventTransition, 250);
		String eventTimestamp = Commons.prepareValueForInsertion(timestamp.toInstant().toString(), 250);

		stmt.execute(
			"INSERT INTO event ( id, trace_id, name, transition, event_coll_id, [time] ) "
			+ "VALUES ( " + eventId + ", " + traceId + ", " + eventName + ", " + eventTransition + ", NULL, " + eventTimestamp + " );"
		);
	}
	
	private static void insertEventAttributes(Statement stmt, long eventId, Collection<XAttribute> attributes, Long parentId) throws SQLException {
		
		for (XAttribute att : attributes) {
			long attributeId = insertAttribute(stmt, att, parentId);
						
			String value = Commons.prepareValueForInsertion(att.toString(), 250);
			
			stmt.execute(
				"INSERT INTO event_has_attribute ( event_id, attr_id, value )"
				+ "VALUES ( " + eventId + ", " + attributeId + ", " + value + " );"
			);
			
			// Recursion over nested attributes
			insertEventAttributes(stmt, eventId, att.getAttributes().values(), attributeId);
		}
	}

	private static long insertAttribute(Statement stmt, XAttribute att, Long parentId) throws SQLException {
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
		return attributeIdQuery.getLong(1);
	}
}

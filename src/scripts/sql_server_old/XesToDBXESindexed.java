package scripts.sql_server_old;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;

import commons.Commons;

public class XesToDBXESindexed {

	private static final String USER = "sa";
	private static final String PWD = "Riva96_shared_db";
	private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
		
	private enum Scope {NONE, EVENT, TRACE};
	
	private static final int MAX_INSERTIONS_IN_BATCH = 1000;
	
	public static void main(String[] args) throws IOException {
		System.out.print("Enter name of the database to populate: ");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String dbName = reader.readLine();
    	String dbUrl = "jdbc:sqlserver://localhost:1433;databaseName=" + dbName + ";encrypt=true;trustServerCertificate=true;";

		File logFile = Commons.selectLogFile();
		if (logFile == null) return;
		
		//File logFile = new File(System.getProperty("user.dir"), "Sepsis Log - 10 traces.xes");
		
		long startTime = System.currentTimeMillis();
		
		System.out.println("Parsing XES file ... ");
		List<XLog> list = Commons.convertToXlog(logFile);
		System.out.println("Complete!");
		
		try (Connection conn = Commons.getConnection(USER, PWD, dbUrl, DRIVER_CLASS)) {
			try (Statement st = conn.createStatement()) {
				// Clearing all data previously contained in the database
				st.execute("EXEC sp_MSForEachTable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL';");
				st.execute("EXEC sp_MSForEachTable 'DELETE FROM ?';");
				st.execute("EXEC sp_MSForEachTable 'ALTER TABLE ? CHECK CONSTRAINT ALL';");
				
				ResultSet resultingId;	// Used to retrieve the id assigned to newly inserted elements in all the tables
				
				// These insertion are grouped for "batch" insertion (1000 records in a single call).
				// It is faster than inserting 1000 times a single record.
				
				List<String[]> logHasExtPendingInsertions = new ArrayList<>();
				List<String[]> logHasClassifPendingInsertions = new ArrayList<>();
				List<String[]> logHasTracePendingInsertions = new ArrayList<>();
				List<String[]> logHasAttribPendingInsertions = new ArrayList<>();
				List<String[]> traceHasAttribPendingInsertions = new ArrayList<>();
				List<String[]> eventHasAttribPendingInsertions = new ArrayList<>();
				
				long logCtr = 1;
				for (XLog log : list) {
					String logName = Commons.prepareValueForInsertion( XConceptExtension.instance().extractName(log), 250);
					
					resultingId = st.executeQuery(
						"INSERT INTO log ( id, name ) "
						+ "OUTPUT INSERTED.id "
						+ "SELECT COALESCE(max(id)+1, 0), " + logName + " FROM log;"
					);
					
					resultingId.next();
					String logId = Commons.prepareValueForInsertion(resultingId.getString(1), Integer.MAX_VALUE);
					
					System.out.println("Log " + logCtr + " - Converting extensions");
					for (XExtension ext : log.getExtensions()) {
						String name = Commons.prepareValueForInsertion(ext.getName(), 50);
						String prefix = Commons.prepareValueForInsertion(ext.getPrefix(), 50);
						String uri = Commons.prepareValueForInsertion(ext.getUri().toString(), 250);
						
						// Insert new extension if not exists, and return the id of the inserted/existent one
						resultingId = st.executeQuery(
							"DECLARE @id TABLE (id BIGINT); "
							
							+ "INSERT INTO extension ( id, name, prefix, uri ) "
							+ "OUTPUT INSERTED.id INTO @id "
							+ "SELECT COALESCE(max(id)+1, 0), " + name + ", " + prefix + ", " + uri + " FROM extension; "
							
							+ "IF NOT EXISTS (SELECT 1 FROM @id) "
								+ "SELECT id FROM extension "
								+ "WHERE name" + Commons.selectPredicate(name) + name
									+ " AND prefix" + Commons.selectPredicate(prefix) + prefix
									+ " AND uri" + Commons.selectPredicate(uri) + uri + " "
							+ "ELSE "
								+ "SELECT * FROM @id;"
						);
							
						resultingId.next();
						String extId = Commons.prepareValueForInsertion(resultingId.getString(1), Integer.MAX_VALUE);
						
						if (logHasExtPendingInsertions.size() < MAX_INSERTIONS_IN_BATCH) {
							logHasExtPendingInsertions.add( new String[]{logId, extId} );
						
						} else {
							
							st.execute(
								"INSERT INTO log_has_ext ( log_id, ext_id ) "
								+ "VALUES ( " + String.join(" ), ( ", logHasExtPendingInsertions.stream().map(arr -> String.join(", ", arr)).collect(Collectors.toList())) + " );"
							);
							
							logHasExtPendingInsertions.clear();
						}
					}
					
					System.out.println("Log " + logCtr + " - Converting classifiers");
					for (XEventClassifier classif : log.getClassifiers()) {
						String name = Commons.prepareValueForInsertion(classif.name(), 50);
						String key = Commons.prepareValueForInsertion(classif.getDefiningAttributeKeys()[0], 250);
						
						// Insert new classifier if not exists, and return the id of the inserted/existent one
						resultingId = st.executeQuery(
							"DECLARE @id TABLE (id BIGINT); "
							
							+ "INSERT INTO classifier ( id, name, [key] ) "
							+ "OUTPUT INSERTED.id INTO @id "
							+ "SELECT COALESCE(max(id)+1, 0), " + name + ", " + key + " FROM classifier; "
							
							+ "IF NOT EXISTS (SELECT 1 FROM @id) "
								+ "SELECT id FROM classifier "
								+ "WHERE name" + Commons.selectPredicate(name) + name
									+ " AND [key]" + Commons.selectPredicate(key) + key + " "
							+ "ELSE "
								+ "SELECT * FROM @id;"
						);
						
						resultingId.next();
						String classifId = Commons.prepareValueForInsertion(resultingId.getString(1), Integer.MAX_VALUE);
						
						if (logHasClassifPendingInsertions.size() < MAX_INSERTIONS_IN_BATCH) {
							logHasClassifPendingInsertions.add( new String[]{logId, classifId} );
						
						} else {
							
							st.execute(
								"INSERT INTO log_has_classifier ( log_id, classifier_id ) "
								+ "VALUES ( " + String.join(" ), ( ", logHasClassifPendingInsertions.stream().map(arr -> String.join(", ", arr)).collect(Collectors.toList())) + " );"
							);
							
							logHasClassifPendingInsertions.clear();
						}
					}
					
					String parentId = Commons.prepareValueForInsertion(null, Integer.MAX_VALUE);
					
					System.out.println("Log " + logCtr + " - Converting attributes");
					for (XAttribute att : log.getAttributes().values())
						if (!att.getKey().equals(XConceptExtension.KEY_NAME))
							populateLogAttributeRelatedTables(st, logHasAttribPendingInsertions, logId, att, parentId, Scope.NONE);
					
					for (XAttribute att : log.getGlobalTraceAttributes())
						populateLogAttributeRelatedTables(st, logHasAttribPendingInsertions, logId, att, parentId, Scope.TRACE);
					
					for (XAttribute att : log.getGlobalEventAttributes())
						populateLogAttributeRelatedTables(st, logHasAttribPendingInsertions, logId, att, parentId, Scope.EVENT);
					
					long traceCtr = 1;
					int logSize = log.size();
					for (XTrace trace : log) {
						String traceName = Commons.prepareValueForInsertion( XConceptExtension.instance().extractName(trace), 250);
						
						resultingId = st.executeQuery(
							"INSERT INTO trace ( id, name ) "
							+ "OUTPUT INSERTED.id "
							+ "SELECT COALESCE(max(id)+1, 0), " + traceName + " FROM trace;"
						);
						
						resultingId.next();
						String traceId = Commons.prepareValueForInsertion(resultingId.getString(1), Integer.MAX_VALUE);
						
						System.out.println("Log " + logCtr + " - Converting trace " + traceCtr + " of " + logSize);
						
						if (logHasTracePendingInsertions.size() < MAX_INSERTIONS_IN_BATCH) {
							logHasTracePendingInsertions.add( new String[]{logId, traceId} );
						
						} else {
							
							st.execute(
								"INSERT INTO log_has_trace ( log_id, trace_id ) "
								+ "VALUES ( " + String.join(" ), ( ", logHasTracePendingInsertions.stream().map(arr -> String.join(", ", arr)).collect(Collectors.toList())) + " );"
							);
							
							logHasTracePendingInsertions.clear();
						}
						
						for (XAttribute att : trace.getAttributes().values())
							if (!att.getKey().equals(XConceptExtension.KEY_NAME))
								populateTraceAttributeRelatedTables(st, traceHasAttribPendingInsertions, traceId, att, parentId);
												
						for (XEvent event : trace) {
							String eventName = Commons.prepareValueForInsertion( XConceptExtension.instance().extractName(event), 250);
							String eventTransition = Commons.prepareValueForInsertion( XLifecycleExtension.instance().extractTransition(event), 50);
							
							// Insert new classifier if not exists, and return the id of the inserted/existent one
							resultingId = st.executeQuery(
								"DECLARE @id TABLE (id BIGINT); "
								
								+ "INSERT INTO activity ( id, name, transition ) "
								+ "OUTPUT INSERTED.id INTO @id "
								+ "SELECT COALESCE(max(id)+1, 0), " + eventName + ", " + eventTransition + " FROM activity; "
								
								+ "IF NOT EXISTS (SELECT 1 FROM @id) "
									+ "SELECT id FROM activity "
									+ "WHERE name" + Commons.selectPredicate(eventName) + eventName
									+ " AND transition" + Commons.selectPredicate(eventTransition) + eventTransition + " "
								+ "ELSE "
									+ "SELECT * FROM @id;"
							);
							
							resultingId.next();
							String activityId = Commons.prepareValueForInsertion(resultingId.getString(1), Integer.MAX_VALUE);
							
							Date timestampDate = XTimeExtension.instance().extractTimestamp(event);
							String eventTimestamp = Commons.prepareValueForInsertion( timestampDate.toInstant().toString(), Integer.MAX_VALUE);
							
							resultingId = st.executeQuery(
								"INSERT INTO event ( id, trace_id, activity_id, event_coll_id, time ) "
								+ "OUTPUT INSERTED.id "
								+ "SELECT COALESCE(max(id)+1, 0), " + traceId + ", " + activityId + ", NULL, " + eventTimestamp + " FROM event; "
							);
							
							resultingId.next();
							String eventId = Commons.prepareValueForInsertion(resultingId.getString(1), Integer.MAX_VALUE);
							
							for (XAttribute att : event.getAttributes().values())
								if (!att.getKey().equals(XConceptExtension.KEY_NAME)
										&& !att.getKey().equals(XLifecycleExtension.KEY_TRANSITION)
										&& !att.getKey().equals(XTimeExtension.KEY_TIMESTAMP))
									populateEventAttributeRelatedTables(st, eventHasAttribPendingInsertions, eventId, att, parentId);
						}
						
						traceCtr++;
					}
					
					logCtr++;
				}
				
				if (!logHasExtPendingInsertions.isEmpty())
					st.execute(
						"INSERT INTO log_has_ext ( log_id, ext_id ) "
						+ "VALUES ( " + String.join(" ), ( ", logHasExtPendingInsertions.stream().map(arr -> String.join(", ", arr)).collect(Collectors.toList())) + " );"
					);
				
				if (!logHasClassifPendingInsertions.isEmpty())
					st.execute(
						"INSERT INTO log_has_classifier ( log_id, classifier_id ) "
						+ "VALUES ( " + String.join(" ), ( ", logHasClassifPendingInsertions.stream().map(arr -> String.join(", ", arr)).collect(Collectors.toList())) + " );"
					);
				
				if (!logHasAttribPendingInsertions.isEmpty())
					st.execute(
						"INSERT INTO log_has_attribute ( log_id, attr_id, value, scope ) "
						+ "VALUES ( " + String.join(" ), ( ", logHasAttribPendingInsertions.stream().map(arr -> String.join(", ", arr)).collect(Collectors.toList())) + " );"
					);
				
				if (!logHasTracePendingInsertions.isEmpty())
					st.execute(
						"INSERT INTO log_has_trace ( log_id, trace_id ) "
						+ "VALUES ( " + String.join(" ), ( ", logHasTracePendingInsertions.stream().map(arr -> String.join(", ", arr)).collect(Collectors.toList())) + " );"
					);
				
				if (!traceHasAttribPendingInsertions.isEmpty())
					st.execute(
						"INSERT INTO trace_has_attribute ( trace_id, attr_id, value ) "
						+ "VALUES ( " + String.join(" ), ( ", traceHasAttribPendingInsertions.stream().map(arr -> String.join(", ", arr)).collect(Collectors.toList())) + " );"
					);
				
				if (!eventHasAttribPendingInsertions.isEmpty())
					st.execute(
						"INSERT INTO event_has_attribute ( event_id, attr_id, value ) "
						+ "VALUES ( " + String.join(" ), ( ", eventHasAttribPendingInsertions.stream().map(arr -> String.join(", ", arr)).collect(Collectors.toList())) + " );"
					);
			}
			
			
			
			long endTime = System.currentTimeMillis();
			double elapsedTime = ((double) (endTime-startTime)) / 1000;
			System.out.println("Succesfully concluded in " + elapsedTime + " seconds");
			
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static void populateLogAttributeRelatedTables(
			Statement stmt,
			List<String[]> logHasAttPendingInsertions,
			String logId,
			XAttribute att,
			String attParentId,
			Scope scope) throws SQLException {
		
		String attId = insertAttributeInTable(stmt, att, attParentId);
		
		String value = Commons.prepareValueForInsertion(att.toString(), 250);
		
		String scopeStr = scope.equals(Scope.NONE) ? null : scope.toString().toLowerCase();
		scopeStr = Commons.prepareValueForInsertion(scopeStr, 50);
		
		if (logHasAttPendingInsertions.size() < MAX_INSERTIONS_IN_BATCH) {
			logHasAttPendingInsertions.add( new String[]{logId, attId, value, scopeStr} );
		
		} else {
			
			stmt.execute(
				"INSERT INTO log_has_attribute ( log_id, attr_id, value, scope ) "
				+ "VALUES ( " + String.join(" ), ( ", logHasAttPendingInsertions.stream().map(arr -> String.join(", ", arr)).collect(Collectors.toList())) + " );"
			);
			
			logHasAttPendingInsertions.clear();
		}
		
		for (XAttribute nested : att.getAttributes().values())
			populateLogAttributeRelatedTables(stmt, logHasAttPendingInsertions, logId, nested, attId, scope);
	}
	
	private static void populateTraceAttributeRelatedTables(
			Statement stmt,
			List<String[]> traceHasAttPendingInsertions,
			String traceId,
			XAttribute att,
			String attParentId) throws SQLException {
		
		String attId = insertAttributeInTable(stmt, att, attParentId);
		
		String value = Commons.prepareValueForInsertion(att.toString(), 250);
		
		
		if (traceHasAttPendingInsertions.size() < MAX_INSERTIONS_IN_BATCH) {
			traceHasAttPendingInsertions.add( new String[]{traceId, attId, value} );
		
		} else {
			
			stmt.execute(
				"INSERT INTO trace_has_attribute ( trace_id, attr_id, value ) "
				+ "VALUES ( " + String.join(" ), ( ", traceHasAttPendingInsertions.stream().map(arr -> String.join(", ", arr)).collect(Collectors.toList())) + " );"
			);
			
			traceHasAttPendingInsertions.clear();
		}
		
		for (XAttribute nested : att.getAttributes().values())
			populateTraceAttributeRelatedTables(stmt, traceHasAttPendingInsertions, traceId, nested, attId);
	}
	
	private static void populateEventAttributeRelatedTables(
			Statement stmt,
			List<String[]> eventHasAttPendingInsertions,
			String eventId,
			XAttribute att,
			String attParentId) throws SQLException {
		
		String attId = insertAttributeInTable(stmt, att, attParentId);
		
		String value = Commons.prepareValueForInsertion(att.toString(), 250);
		
		if (eventHasAttPendingInsertions.size() < MAX_INSERTIONS_IN_BATCH) {
			eventHasAttPendingInsertions.add( new String[]{eventId, attId, value} );
		
		} else {
			
			stmt.execute(
				"INSERT INTO event_has_attribute ( event_id, attr_id, value ) "
				+ "VALUES ( " + String.join(" ), ( ", eventHasAttPendingInsertions.stream().map(arr -> String.join(", ", arr)).collect(Collectors.toList())) + " );"
			);
			
			eventHasAttPendingInsertions.clear();
		}
		
		for (XAttribute nested : att.getAttributes().values())
			populateEventAttributeRelatedTables(stmt, eventHasAttPendingInsertions, eventId, nested, attId);
	}
	
	private static String insertAttributeInTable(Statement stmt, XAttribute att, String parentId) throws SQLException {
		String type;
		if (att instanceof XAttributeBoolean)
			type = "'boolean'";
		else if (att instanceof XAttributeContinuous)
			type = "'continuous'";
		else if (att instanceof XAttributeDiscrete)
			type = "'discrete'";
		else	// Treating all other types as literals
			type = "'literal'";

		String key = Commons.prepareValueForInsertion(att.getKey(), 50);
		
		String extId;
		if (att.getExtension() != null) {
			String extName = Commons.prepareValueForInsertion(att.getExtension().getName(), 50);
			String extPrefix = Commons.prepareValueForInsertion(att.getExtension().getPrefix(), 50);
			String extUri = Commons.prepareValueForInsertion(att.getExtension().getUri().toString(), 250);
			
			ResultSet extIdQueryResult = stmt.executeQuery(
				"SELECT id FROM extension "
				+ "WHERE name" + Commons.selectPredicate(extName) + extName
					+ " AND prefix" + Commons.selectPredicate(extPrefix) + extPrefix
					+ " AND uri" + Commons.selectPredicate(extUri) + extUri + ";"
			);
			
			extIdQueryResult.next();
			extId = "'" + extIdQueryResult.getString(1) + "'";
		
		} else {
			extId = "NULL";
		}
		
		// Insert new attribute if not exists, and return the id of the inserted/existent one
		ResultSet resultingId = stmt.executeQuery(
			"DECLARE @id TABLE (id BIGINT); "
			
			+ "INSERT INTO attribute ( id, type, [key], ext_id, parent_id ) "
			+ "OUTPUT INSERTED.id INTO @id "
			+ "SELECT COALESCE(max(id)+1, 0), " + type + ", " + key + ", " + extId + ", " + parentId + " FROM attribute; "
			
			+ "IF NOT EXISTS (SELECT 1 FROM @id) "
				+ "SELECT id FROM attribute "
				+ "WHERE type" + Commons.selectPredicate(type) + type
					+ " AND [key]" + Commons.selectPredicate(key) + key
					+ " AND ext_id" + Commons.selectPredicate(extId) + extId
					+ " AND parent_id" + Commons.selectPredicate(parentId) + parentId + ";"
			+ "ELSE "
				+ "SELECT * FROM @id;"
		);
		
		resultingId.next();
		return Commons.prepareValueForInsertion(resultingId.getString(1), Integer.MAX_VALUE);
	}
}

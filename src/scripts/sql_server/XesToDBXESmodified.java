package scripts.sql_server;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

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

public class XesToDBXESmodified {
	
	private static final String SCHEMA_NAME = "dbxes_modified";
	private static final String DB_URL = "jdbc:sqlserver://localhost:1433;databaseName=" + SCHEMA_NAME + ";encrypt=true;trustServerCertificate=true;";
	private static final String USER = "sa";
	private static final String PWD = "Riva96_shared_db";
	private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	
	private enum Scope {NONE, EVENT, TRACE};

	public static void main(String[] args) {
		File logFile = Commons.selectLogFile();
		if (logFile == null) return;
		
		//File logFile = new File(System.getProperty("user.dir"), "Sepsis Log - 10 traces.xes");
		
		long startTime = System.currentTimeMillis();
		
		List<XLog> list = Commons.convertToXlog(logFile);
		
		try (Connection conn = Commons.getConnection(USER, PWD, DB_URL, DRIVER_CLASS)) {
			try (Statement st = conn.createStatement()) {
				// Clearing all data previously contained in the database
				st.execute("EXEC sp_MSForEachTable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL';");
				st.execute("EXEC sp_MSForEachTable 'DELETE FROM ?';");
				st.execute("EXEC sp_MSForEachTable 'ALTER TABLE ? CHECK CHECK CONSTRAINT ALL';");
				st.execute("EXEC sp_MSforeachtable 'DBCC CHECKIDENT(''?'', RESEED, -1)'");
				
				ResultSet resultingId;	// Used to retrieve the id assigned to newly inserted elements in all the tables
				
				for (XLog log : list) {
					String logName = Commons.prepareValueForInsertion( XConceptExtension.instance().extractName(log), 250);
					
					resultingId = st.executeQuery(
						"INSERT INTO log ( name ) "
						+ "OUTPUT INSERTED.ID "
						+ "VALUES ( " + logName + " );"
					);
					
					resultingId.next();
					long logId = Long.parseLong(resultingId.getString(1));
					
					System.out.println("Log " + (logId+1) + " - Converting extensions");
					for (XExtension ext : log.getExtensions()) {
						String name = Commons.prepareValueForInsertion(ext.getName(), 50);
						String prefix = Commons.prepareValueForInsertion(ext.getPrefix(), 50);
						String uri = Commons.prepareValueForInsertion(ext.getUri().toString(), 250);
						
						// Check if the extension is already present in the table
						resultingId = st.executeQuery(
							"SELECT id FROM extension "
							+ "WHERE name" + Commons.selectPredicate(name) + name
								+ " AND prefix" + Commons.selectPredicate(prefix) + prefix
								+ " AND uri" + Commons.selectPredicate(uri) + uri + ";"
						);
						
						if (!resultingId.next()) {	// Means that the extension isn't present yet and should be inserted
							resultingId = st.executeQuery(
								"INSERT INTO extension ( name, prefix, uri ) "
								+ "OUTPUT INSERTED.ID "
								+ "VALUES ( " + name + ", " + prefix + ", " + uri + " );"
							);
							
							resultingId.next();
						}
						
						long extId = Long.parseLong(resultingId.getString(1));
						
						st.execute(
							"INSERT INTO log_has_ext ( log_id, ext_id ) "
							+ "VALUES ( '" + logId + "', '" + extId + "' );"
						);
					}
					
					System.out.println("Log " + (logId+1) + " - Converting classifiers");
					for (XEventClassifier classif : log.getClassifiers()) {
						String name = Commons.prepareValueForInsertion(classif.name(), 50);
						String key = Commons.prepareValueForInsertion(classif.getDefiningAttributeKeys()[0], 250);
						
						// Check if the classifier is already present in the table
						resultingId = st.executeQuery(
							"SELECT id FROM classifier "
							+ "WHERE name" + Commons.selectPredicate(name) + name
								+ " AND [key]" + Commons.selectPredicate(key) + key + ";"
						);
						
						if (!resultingId.next()) {	// Means that the classifier isn't present yet and should be inserted
							resultingId = st.executeQuery(
								"INSERT INTO classifier ( name, [key] ) "
								+ "OUTPUT INSERTED.ID "
								+ "VALUES ( " + name + ", " + key + " );"
							);
							
							resultingId.next();
						}
						
						long classifId = Long.parseLong(resultingId.getString(1));
						
						st.execute(
							"INSERT INTO log_has_classifier ( log_id, classifier_id ) "
							+ "VALUES ( '" + logId + "', '" + classifId + "' );"
						);
					}
					
					System.out.println("Log " + (logId+1) + " - Converting attributes");
					for (XAttribute att : log.getAttributes().values())
						if (!att.getKey().equals(XConceptExtension.KEY_NAME))
							populateAttributeAndRelatedTables("log", logId, att, -1, st, Scope.NONE);
					
					for (XAttribute att : log.getGlobalTraceAttributes())
						populateAttributeAndRelatedTables("log", logId, att, -1, st, Scope.TRACE);
					
					for (XAttribute att : log.getGlobalEventAttributes())
						populateAttributeAndRelatedTables("log", logId, att, -1, st, Scope.EVENT);
											
					
					int logSize = log.size();
					for (XTrace trace : log) {
						String traceName = Commons.prepareValueForInsertion( XConceptExtension.instance().extractName(trace), 250);
						
						resultingId = st.executeQuery(
							"INSERT INTO trace ( name ) "
							+ "OUTPUT INSERTED.ID "
							+ "VALUES ( " + traceName + " );"
						);
						
						resultingId.next();
						long traceId = Long.parseLong(resultingId.getString(1));
						
						System.out.println("Log " + (logId+1) + " - Converting trace " + (traceId+1) + " of " + logSize);
						
						st.execute(
							"INSERT INTO log_has_trace ( log_id, trace_id ) "
							+ "VALUES ( '" + logId + "', '" + traceId+ "' );"
						);
						
						for (XAttribute att : trace.getAttributes().values())
							if (!att.getKey().equals(XConceptExtension.KEY_NAME))
								populateAttributeAndRelatedTables("trace", traceId, att, -1, st, null);
						
						
						for (XEvent event : trace) {
							String eventName = Commons.prepareValueForInsertion( XConceptExtension.instance().extractName(event), 250);
							String eventTransition = Commons.prepareValueForInsertion( XLifecycleExtension.instance().extractTransition(event), 50);
							
							SimpleDateFormat sdf = new SimpleDateFormat();
							sdf.applyPattern("YYYY-MM-dd HH:mm:ss.SSS XXX");
							Date eventTimestamp = XTimeExtension.instance().extractTimestamp(event);
							
							String timestampStr = Commons.prepareValueForInsertion( sdf.format(eventTimestamp), 50);
							
							resultingId = st.executeQuery(
								"INSERT INTO event ( name, transition, time, event_coll_id ) "
								+ "OUTPUT INSERTED.ID "
								+ "VALUES ( " + eventName + ", " + eventTransition + ", " + timestampStr + ", NULL );"
							);
							
							resultingId.next();
							long eventId = Long.parseLong(resultingId.getString(1));
							
							st.execute(
								"INSERT INTO trace_has_event ( trace_id, event_id ) "
								+ "VALUES ( '" + traceId + "', '" + eventId + "' );"
							);
							
							for (XAttribute att : event.getAttributes().values())
								if (!att.getKey().equals(XConceptExtension.KEY_NAME)
										&& !att.getKey().equals(XLifecycleExtension.KEY_TRANSITION)
										&& !att.getKey().equals(XTimeExtension.KEY_TIMESTAMP))
									populateAttributeAndRelatedTables("event", eventId, att, -1, st, null);
						}
					}
				}
			}
			
			long endTime = System.currentTimeMillis();
			double elapsedTime = ((double) (endTime-startTime)) / 1000;
			System.out.println("Succesfully concluded in " + elapsedTime + " seconds");
			
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static void populateAttributeAndRelatedTables(
			String relatedTableName, 
			long relatedTableId, 
			XAttribute att, 
			long parentId, 
			Statement stmt, 
			Scope scope) throws SQLException {
		
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
		String value = Commons.prepareValueForInsertion(att.toString(), 250);
		
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
		
		String parentIdStr = parentId<0 ? "NULL" : ("'" + parentId + "'");
		
		// Check if the attribute is already present in the table
		ResultSet resultingId = stmt.executeQuery(
			"SELECT id FROM attribute "
			+ "WHERE type" + Commons.selectPredicate(type) + type
				+ " AND [key]" + Commons.selectPredicate(key) + key
				+ " AND [value]" + Commons.selectPredicate(value) + value
				+ " AND ext_id" + Commons.selectPredicate(extId) + extId
				+ " AND parent_id" + Commons.selectPredicate(parentIdStr) + parentIdStr + ";"
		);
		
		if (!resultingId.next()) {	// Means that the attribute isn't present yet and should be inserted
			List<String> values = List.of(type, key, value, extId, parentIdStr);
			resultingId = stmt.executeQuery(
				"INSERT INTO attribute ( type, [key], [value], ext_id, parent_id ) "
				+ "OUTPUT INSERTED.ID "
				+ "VALUES ( " + String.join(", ", values) + " );"
			);
			
			resultingId.next();
		}
		
		long attId = Long.valueOf(resultingId.getString(1));
		
		ResultSet existenceCheck = stmt.executeQuery(
			"SELECT 1 FROM " + relatedTableName+"_has_attribute "
			+ "WHERE " + relatedTableName+"_id = '" + relatedTableId + "' AND attr_id = '" + attId + "' ;"
		);
		
		boolean isAttributeExisting = existenceCheck.next();
		
		switch (relatedTableName) {
		case "log":
			String scopeStr;
			if (scope.equals(Scope.NONE))
				scopeStr = null;
			else
				scopeStr = scope.toString().toLowerCase();
			
			scopeStr = Commons.prepareValueForInsertion(scopeStr, 50);
			
			if (!isAttributeExisting)
				stmt.execute(
					"INSERT INTO log_has_attribute ( log_id, attr_id, scope ) "
					+ "VALUES ( '" + relatedTableId + "', '" + attId + "', " + scopeStr + " );"
				);
			
			break;

		case "trace":
			if (!isAttributeExisting)
				stmt.execute(
					"INSERT INTO trace_has_attribute ( trace_id, attr_id ) "
					+ "VALUES ( '" + relatedTableId + "', '" + attId + "' );"
				);
			break;
			
		case "event":
			if (!isAttributeExisting)
				stmt.execute(
					"INSERT INTO event_has_attribute ( event_id, attr_id ) "
					+ "VALUES ( '" + relatedTableId + "', '" + attId + "' );"
				);
			break;
		}
		
		
		long newParentId = attId;
		for (XAttribute nested : att.getAttributes().values())
			populateAttributeAndRelatedTables(relatedTableName, relatedTableId, nested, newParentId, stmt, scope);
	}
}

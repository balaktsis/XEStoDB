package scripts.sql_server_old;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeTimestamp;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;

import commons.Commons;

public class XesToDBXES {
	
	private static final String USER = "sa";
	private static final String PWD = "Riva96_shared_db";
	private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	
	private enum Scope {NONE, EVENT, TRACE};

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
				
				for (XLog log : list) {
					resultingId = st.executeQuery("SELECT MAX( CAST(id AS BIGINT) ) FROM log ");
					resultingId.next();
					long logId = resultingId.getString(1) == null ? 0 : Long.parseLong(resultingId.getString(1))+1;
					
					st.execute(
						"INSERT INTO log ( id ) "
						+ "VALUES ( '" + logId + "' );"
					);
					
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
						
						if (resultingId.next()) {	// Means that the extension is already present
							st.execute(
								"INSERT INTO log_has_ext ( log_id, ext_id ) "
								+ "VALUES ( '" + logId + "', '" + resultingId.getString(1) + "' );"
							);
							
						} else {	// The extension should be inserted
							resultingId = st.executeQuery("SELECT MAX( CAST(id AS BIGINT) ) FROM extension ");
							resultingId.next();
							long extId = resultingId.getString(1) == null ? 0 : Long.parseLong(resultingId.getString(1))+1;
							
							st.execute(
								"INSERT INTO extension ( id, name, prefix, uri ) "
								+ "VALUES ( '" + extId + "', " + name + ", " + prefix + ", " + uri + " );"
							);
							
							st.execute(
								"INSERT INTO log_has_ext ( log_id, ext_id ) "
								+ "VALUES ( '" + logId + "', '" + extId + "' );"
							);
							
							extId++;
						}
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
						
						if (resultingId.next()) {	// Means that the classifier is already present
							st.execute(
								"INSERT INTO log_has_classifier ( log_id, classifier_id ) "
								+ "VALUES ( '" + logId + "', '" + resultingId.getString(1) + "' );"
							);
							
						} else {	// The classifier should be inserted
							resultingId = st.executeQuery("SELECT MAX( CAST(id AS BIGINT) ) FROM classifier ");
							resultingId.next();
							long classifId = resultingId.getString(1) == null ? 0 : Long.parseLong(resultingId.getString(1))+1;
							
							st.execute(
								"INSERT INTO classifier ( id, name, [key] ) "
								+ "VALUES ( '" + classifId + "', " + name + ", " + key + " );"
							);
							
							st.execute(
								"INSERT INTO log_has_classifier ( log_id, classifier_id ) "
								+ "VALUES ( '" + logId + "', '" + classifId + "' );"
							);
							
							classifId++;
						}
					}
					
					System.out.println("Log " + (logId+1) + " - Converting attributes");
					for (XAttribute att : log.getAttributes().values())
						populateLogAttributeRelatedTables(st, logId, att, -1, Scope.NONE);
					
					for (XAttribute att : log.getGlobalTraceAttributes())
						populateLogAttributeRelatedTables(st, logId, att, -1, Scope.TRACE);
					
					for (XAttribute att : log.getGlobalEventAttributes())
						populateLogAttributeRelatedTables(st, logId, att, -1, Scope.EVENT);
											
					
					int logSize = log.size();
					for (XTrace trace : log) {
						resultingId = st.executeQuery("SELECT MAX( CAST(id AS BIGINT) ) FROM trace ");
						resultingId.next();
						long traceId = resultingId.getString(1) == null ? 0 : Long.parseLong(resultingId.getString(1))+1;
						
						System.out.println("Log " + (logId+1) + " - Converting trace " + (traceId+1) + " of " + logSize);
						
						st.execute(
							"INSERT INTO trace ( id ) "
							+ "VALUES ( '" + traceId + "' );"
						);
						
						st.execute(
							"INSERT INTO log_has_trace ( log_id, trace_id ) "
							+ "VALUES ( '" + logId + "', '" + traceId + "' );"
						);
						
						for (XAttribute att : trace.getAttributes().values())
							populateTraceAttributeRelatedTables(st, traceId, att, -1);
						
						
						for (XEvent event : trace) {
							resultingId = st.executeQuery("SELECT MAX( CAST(id AS BIGINT) ) FROM event ");
							resultingId.next();
							long eventId = resultingId.getString(1) == null ? 0 : Long.parseLong(resultingId.getString(1))+1;
							
							st.execute(
								"INSERT INTO event ( id, event_coll_id ) "
								+ "VALUES ( '" + eventId + "', NULL );"
							);
							
							st.execute(
								"INSERT INTO trace_has_event ( trace_id, event_id ) "
								+ "VALUES ( '" + traceId + "', '" + eventId + "' );"
							);
							
							for (XAttribute att : event.getAttributes().values())
								populateEventAttributeRelatedTables(st, eventId, att, -1);
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
	
	private static void populateLogAttributeRelatedTables(
			Statement stmt,
			long logId,
			XAttribute att,
			long attParentId,
			Scope scope) throws SQLException {
		
		long attId = insertAttributeInTable(stmt, att, attParentId);
		
		String scopeStr = scope.equals(Scope.NONE) ? null : scope.toString().toLowerCase();
		scopeStr = Commons.prepareValueForInsertion(scopeStr, 50);
		
		ResultSet existenceCheck = stmt.executeQuery(
			"SELECT 1 FROM log_has_attribute "
			+ "WHERE log_id = '" + logId + "'"
				+ " AND attr_id = '" + attId + "';"
		);
			
		if (!existenceCheck.next())
			stmt.execute(
				"INSERT INTO log_has_attribute ( log_id, attr_id, scope ) "
				+ "VALUES ( '" + logId + "', '" + attId + "', " + scopeStr + " );"
			);
		
		long newParentId = attId;
		for (XAttribute nested : att.getAttributes().values())
			populateLogAttributeRelatedTables(stmt, logId, nested, newParentId, scope);
	}
	
	private static void populateTraceAttributeRelatedTables(
			Statement stmt,
			long traceId,
			XAttribute att,
			long attParentId) throws SQLException {
		
		long attId = insertAttributeInTable(stmt, att, attParentId);
		
		ResultSet existenceCheck = stmt.executeQuery(
			"SELECT 1 FROM trace_has_attribute "
			+ "WHERE trace_id = '" + traceId + "'"
				+ " AND attr_id = '" + attId + "';"
		);
			
		if (!existenceCheck.next())
			stmt.execute(
				"INSERT INTO trace_has_attribute ( trace_id, attr_id ) "
				+ "VALUES ( '" + traceId + "', '" + attId + "' );"
			);
		
		long newParentId = attId;
		for (XAttribute nested : att.getAttributes().values())
			populateTraceAttributeRelatedTables(stmt, traceId, nested, newParentId);
	}
	
	private static void populateEventAttributeRelatedTables(
			Statement stmt,
			long eventId,
			XAttribute att,
			long attParentId) throws SQLException {
		
		/*
		SimpleDateFormat sdf = new SimpleDateFormat();
		sdf.applyPattern("YYYY-MM-dd HH:mm:ss.SSS XXX");
		Date eventTimestamp = XTimeExtension.instance().extractTimestamp(event);
		 */
		
		long attId = insertAttributeInTable(stmt, att, attParentId);
		
		ResultSet existenceCheck = stmt.executeQuery(
			"SELECT 1 FROM event_has_attribute "
			+ "WHERE event_id = '" + eventId + "'"
				+ " AND attr_id = '" + attId + "';"
		);
			
		if (!existenceCheck.next())
			stmt.execute(
				"INSERT INTO event_has_attribute ( event_id, attr_id ) "
				+ "VALUES ( '" + eventId + "', '" + attId + "' );"
			);
		
		long newParentId = attId;
		for (XAttribute nested : att.getAttributes().values())
			populateEventAttributeRelatedTables(stmt, eventId, nested, newParentId);
	}
	
	private static long insertAttributeInTable(Statement stmt, XAttribute att, long parentId) throws SQLException {
		String type;
		if (att instanceof XAttributeBoolean)
			type = "boolean";
		else if (att instanceof XAttributeContinuous)
			type = "continuous";
		else if (att instanceof XAttributeDiscrete)
			type = "discrete";
		else if (att instanceof XAttributeTimestamp)
			type = "timestamp";
		else	// Treating all other types as literals
			type = "literal";

		type = Commons.prepareValueForInsertion(type, 50);
		
		String key = Commons.prepareValueForInsertion(att.getKey(), 50);
		
		String value;
		if (att instanceof XAttributeTimestamp) {
			XAttributeTimestamp dateAtt = (XAttributeTimestamp) att;
			value  = Commons.prepareValueForInsertion(dateAtt.getValue().toInstant().toString(), 250);
		} else
			value  = Commons.prepareValueForInsertion(att.toString(), 250);
		
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
		
		if (!resultingId.next()) {	// Means that the attribute isn't present yet
			resultingId = stmt.executeQuery("SELECT MAX( CAST(id AS BIGINT) ) FROM attribute ");
			resultingId.next();
			long attId = resultingId.getString(1) == null ? 0 : Long.parseLong(resultingId.getString(1))+1;
			
			stmt.execute(
				"INSERT INTO attribute ( id, type, [key], [value], ext_id, parent_id ) "
				+ "VALUES ( '" + attId + "', " + type + ", " + key + ", " + value + ", " + extId + ", " + parentIdStr + " );"
			);
			
			return attId;
		
		} else {
			return Long.valueOf(resultingId.getString(1));
		}
	}
}

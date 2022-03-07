package scripts.my_sql;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
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

public class XesToDBXESmodified2 {
	
	private static final String SCHEMA_NAME = "DB-XES_modified2";
	private static final String DB_URL = "jdbc:mysql://localhost:3306/" + SCHEMA_NAME;
	private static final String USER = "root";
	private static final String PWD = "password";
	private static final String DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

	private enum Scope {NONE, EVENT, TRACE};
	
	private static String name;
	private static String timestamp;
	private static String transition;
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
	
	public static void main(String[] args) {
		File logFile = Commons.selectLogFile();
		if (logFile == null) return;
		
		long startTime = System.currentTimeMillis();
		
		List<XLog> list = Commons.convertToXlog(logFile);
		
		try (Connection conn = Commons.getConnection(USER, PWD, DB_URL, DRIVER_CLASS)) {
			try (Statement st = conn.createStatement()) {
				
				ResultSet rs = st.executeQuery(
					"SELECT TABLE_NAME "
					+ "FROM information_schema.TABLES "
					+ "WHERE TABLE_SCHEMA = '" + SCHEMA_NAME + "';"
				);
				
				try (Statement delete = conn.createStatement()) {
					delete.execute("SET SQL_SAFE_UPDATES=OFF;");
					delete.execute("SET FOREIGN_KEY_CHECKS=OFF;");
					
					while (rs.next())
						delete.execute("DELETE FROM " + rs.getString(1) + ";");
					
					delete.execute("SET FOREIGN_KEY_CHECKS=ON;");
					delete.execute("SET SQL_SAFE_UPDATES=ON;");
				}
				
				long logID = 0;
				long traceID = 0;
				long eventID = 0;
				long extID = 0;
				long attID = 0;
				long classifID = 0;
				
				for (XLog log : list) {
					
					System.out.println("Log " + (logID+1) + " - Converting extensions");
					for (XExtension ext : log.getExtensions()) {
						String name = ext.getName();
						String prefix = ext.getPrefix();
						String uri = ext.getUri().toString();
						
						st.execute(
							"INSERT INTO extension ( id, name, prefix, uri ) "
							+ "VALUES ( '" + extID + "', '" + name + "', '" + prefix + "', '" + uri + "' );"
						);
						
						extID++;
					}
					
					System.out.println("Log " + (logID+1) + " - Converting attributes");
					name = XConceptExtension.instance().extractName(log);
					if (log.getAttributes().values().stream().filter(att -> !att.getKey().equals(XConceptExtension.KEY_NAME)).count() > 0) {
						for (XAttribute att : log.getAttributes().values())
							if (!att.getKey().equals(XConceptExtension.KEY_NAME))
								attID = populateAttributeAndRelatedTable("log", logID, -1, attID, att, -1, st, Scope.NONE);
					
					} else {
						st.execute(
							"INSERT INTO log ( id, name, attr_id ) "
							+ "VALUES ( '" + logID + "', '" + name + "', NULL );"
						);
					}
					
					for (XAttribute att : log.getGlobalTraceAttributes())
						attID = populateAttributeAndRelatedTable("log", logID, -1, attID, att, -1, st, Scope.TRACE);
					
					for (XAttribute att : log.getGlobalEventAttributes())
						attID = populateAttributeAndRelatedTable("log", logID, -1, attID, att, -1, st, Scope.EVENT);
					
					
					for (int i=0; i<extID; i++)
						st.execute(
							"INSERT INTO log_has_ext ( log_id, ext_id ) "
							+ "VALUES ( '" + logID + "', '" + i + "' );"
						);
					
					
					System.out.println("Log " + (logID+1) + " - Converting classifiers");
					for (XEventClassifier classif : log.getClassifiers()) {
						
						st.execute(
							"INSERT INTO classifier ( id, name, key_ ) "
							+ "VALUES ( '" + classifID + "', '" + classif.name() + "', '" + classif.getDefiningAttributeKeys()[0] + "' );"
						);
						
						st.execute(
							"INSERT INTO log_has_classifier ( log_id, classifier_id ) "
							+ "VALUES ( '" + logID + "', '" + classifID + "' );"
						);
						
						classifID++;
					}
					
					
					int logSize = log.size();
					for (XTrace trace : log) {
						System.out.println("Log " + (logID+1) + " - Converting trace " + (traceID+1) + " of " + logSize);
						
						name = XConceptExtension.instance().extractName(trace);
						if (trace.getAttributes().values().stream().filter(att -> !att.getKey().equals(XConceptExtension.KEY_NAME)).count() > 0) {
						
							for (XAttribute att : trace.getAttributes().values())
								if (!att.getKey().equals(XConceptExtension.KEY_NAME))
									attID = populateAttributeAndRelatedTable("trace", traceID, logID, attID, att, -1, st, null);
						
						} else {
							st.execute(
								"INSERT INTO trace ( id, log_id, name, attr_id ) "
								+ "VALUES ( '" + traceID + "', '" + logID + "', '" + name + "', NULL );"
							);
						}
						
						for (XEvent event : trace) {
							name = XConceptExtension.instance().extractName(event);
							timestamp = sdf.format( XTimeExtension.instance().extractTimestamp(event) );
							transition = XLifecycleExtension.instance().extractTransition(event);
							if (log.getAttributes().values().stream().filter(att -> !att.getKey().equals(XConceptExtension.KEY_NAME) && !att.getKey().equals(XTimeExtension.KEY_TIMESTAMP) && !att.getKey().equals(XLifecycleExtension.KEY_TRANSITION)).count() > 0) {
								for (XAttribute att : event.getAttributes().values())
									if (!att.getKey().equals(XConceptExtension.KEY_NAME)
											&& !att.getKey().equals(XTimeExtension.KEY_TIMESTAMP)
											&& !att.getKey().equals(XLifecycleExtension.KEY_TRANSITION))
										attID = populateAttributeAndRelatedTable("event", eventID, traceID, attID, att, -1, st, null);
							} else {
								st.execute(
									"INSERT INTO event ( id, trace_id, name, transition, timestamp, attr_id, event_coll_id ) "
									+ "VALUES ( '" + eventID + "', '" + traceID + "', '" + name + "', '" + transition + "', '" + timestamp + "', NULL, NULL );"
								);
							}
							
							eventID++;
						}

						traceID++;
					}
					
					logID++;
				}
				
			}
			
			long endTime = System.currentTimeMillis();
			double elapsedTime = ((double) (endTime-startTime)) / 1000;
			System.out.println("Succesfully concluded in " + elapsedTime + " seconds");
			
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static long populateAttributeAndRelatedTable(
			String relatedTableName, 
			long relatedTableID,
			long parentTableID,
			long attID, 
			XAttribute att, 
			long parentID, 
			Statement stmt, 
			Scope scope) throws SQLException {
		
		String type;
		if (att instanceof XAttributeBoolean)
			type = "'boolean'";
		else if (att instanceof XAttributeContinuous)
			type = "'double'";
		else if (att instanceof XAttributeDiscrete)
			type = "'bigint'";
		else	// Treating all other types as literals
			type = "'varchar'";

		String key = "'" + att.getKey().substring(0, Math.min(att.getKey().length(), 50)) + "'";
		String value = "'" + att.toString().substring(0, Math.min(att.toString().length(), 250)) + "'";
		
		String extID;
		if (att.getExtension() != null) {
			ResultSet extIdQueryResult = stmt.executeQuery(
				"SELECT id "
				+ "FROM extension "
				+ "WHERE name='" + att.getExtension().getName() + "' "
					+ "and prefix='" + att.getExtension().getPrefix() + "' "
					+ "and uri='" + att.getExtension().getUri().toString() + "';"
			);
			
			extIdQueryResult.next();
			extID = "'" + extIdQueryResult.getString(1) + "'";
		
		} else {
			extID = "NULL";
		}
		
		String parentIDstr = parentID<0 ? "NULL" : ("'" + parentID + "'");
		
		List<String> values = List.of(String.valueOf(attID), type, key, value, extID, parentIDstr);
		stmt.execute(
			"INSERT INTO attribute ( id, type, key_, value_, ext_id, parent_id ) "
			+ "VALUES ( " + String.join(", ", values) + " );"
		);
		
		switch (relatedTableName) {
		case "log":
			stmt.execute(
				"INSERT INTO log ( id, name, attr_id ) "
				+ "VALUES ( '" + relatedTableID + "', '" + name + "', '" + attID + "' );"
			);
			
			if (!scope.equals(Scope.NONE)) {
				stmt.execute(
					"INSERT INTO log_has_global ( log_id, attr_id, scope ) "
					+ "VALUES ( '" + relatedTableID + "', '" + attID + "', '" + scope.toString().toLowerCase() + "' );"
				);
			}
			break;

		case "trace":
			stmt.execute(
				"INSERT INTO trace ( id, log_id, name, attr_id ) "
				+ "VALUES ( '" + relatedTableID + "', '" + parentTableID + "', '" + name + "', '" + attID + "' );"
			);
			break;
			
		case "event":
			stmt.execute(
				"INSERT INTO event ( id, trace_id, name, transition, timestamp, attr_id, event_coll_id ) "
				+ "VALUES ( '" + relatedTableID + "', '" + parentTableID + "', '" + name + "', '" + transition + "', '" + timestamp + "', '" + attID + "', NULL );"
			);
			break;
		}
		
		
		long newParentID = attID;
		for (XAttribute nested : att.getAttributes().values())
			attID = populateAttributeAndRelatedTable(relatedTableName, relatedTableID, parentTableID, ++attID, nested, newParentID, stmt, scope);
		
		return parentID<0 ? attID+1 : attID;
	}
}

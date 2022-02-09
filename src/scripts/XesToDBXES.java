package scripts;

import java.io.File;
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
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;

public class XesToDBXES {
	
	private static final String DB_URL = "jdbc:mysql://localhost:3306/DB-XES_without_DFR";
	private static final String USER = "root";
	private static final String PWD = "password";
	
	private enum Scope {NONE, EVENT, TRACE};

	public static void main(String[] args) {
		File logFile = Commons.selectLogFile();
		if (logFile == null) return;
		
		long startTime = System.currentTimeMillis();
		
		List<XLog> list = Commons.convertToXlog(logFile);
		
		try (Connection conn = Commons.getConnection(USER, PWD, DB_URL)) {
			try (Statement st = conn.createStatement()) {
				
				ResultSet rs = st.executeQuery(
					"SELECT TABLE_NAME "
					+ "FROM information_schema.TABLES "
					+ "WHERE TABLE_SCHEMA = 'DB-XES_without_DFR';"
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
					for (XAttribute att : log.getAttributes().values())
						attID = populateAttributeAndRelatedTable("log", logID, attID, att, -1, st, Scope.NONE);
					
					for (XAttribute att : log.getGlobalTraceAttributes())
						attID = populateAttributeAndRelatedTable("log", logID, attID, att, -1, st, Scope.TRACE);
					
					for (XAttribute att : log.getGlobalEventAttributes())
						attID = populateAttributeAndRelatedTable("log", logID, attID, att, -1, st, Scope.EVENT);
					
					
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
						
						for (XAttribute att : trace.getAttributes().values())
							attID = populateAttributeAndRelatedTable("trace", traceID, attID, att, -1, st, null);
						
						st.execute(
							"INSERT INTO log_has_trace ( log_id, trace_id, sequence ) "
							+ "VALUES ( '" + logID + "', '" + traceID + "', NULL );"
						);
						
						
						for (XEvent event : trace) {
							for (XAttribute att : event.getAttributes().values())
								attID = populateAttributeAndRelatedTable("event", eventID, attID, att, -1, st, null);
							
							st.execute(
								"INSERT INTO trace_has_event ( trace_id, event_id, sequence ) "
								+ "VALUES ( '" + traceID + "', '" + eventID + "', NULL );"
							);
							
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
				"INSERT INTO log ( id, attr_id ) "
				+ "VALUES ( '" + relatedTableID + "', '" + attID + "' );"
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
				"INSERT INTO trace ( id, attr_id ) "
				+ "VALUES ( '" + relatedTableID + "', '" + attID + "' );"
			);
			break;
			
		case "event":
			stmt.execute(
				"INSERT INTO event ( id, attr_id, event_coll_id ) "
				+ "VALUES ( '" + relatedTableID + "', '" + attID + "', NULL );"
			);
			break;
		}
		
		
		long newParentID = attID;
		for (XAttribute nested : att.getAttributes().values())
			attID = populateAttributeAndRelatedTable(relatedTableName, relatedTableID, ++attID, nested, newParentID, stmt, scope);
		
		return parentID<0 ? attID+1 : attID;
	}

}

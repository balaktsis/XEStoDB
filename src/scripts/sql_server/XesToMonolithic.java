package scripts.sql_server;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.deckfour.xes.classification.XEventClassifier;
import org.deckfour.xes.extension.XExtension;
import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.extension.std.XLifecycleExtension;
import org.deckfour.xes.extension.std.XTimeExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;

import commons.Commons;

public class XesToMonolithic {
	
	private static final String USER = "sa";
	private static final String PWD = "Riva96_shared_db";
	private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	
	private static final int VARCHAR_LEN = 250;

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
				
				System.out.println("Creating monolithic table");
				
				String tableName = "log";	//Commons.getNameForDB(logFile.getName());
				
				st.execute("DROP TABLE IF EXISTS " + tableName + ";");
				
				st.execute(	// Simplified table of a real log
					"CREATE TABLE " + tableName + "("
					+ "log_id 		bigint		NOT NULL,"
					+ "log_name 	varchar(" + VARCHAR_LEN + ")	NULL"
					+ ");"
				);
				
				Set<String> extensionNames = new LinkedHashSet<>();
				Set<String> classifierNames = new LinkedHashSet<>();
				Set<String> logGlobAttNames = new LinkedHashSet<>();
				Set<String> logAttNames = new LinkedHashSet<>();
				Set<String> trcAttNames = new LinkedHashSet<>();
				Set<String> evtAttNames = new LinkedHashSet<>();
				
				for (XLog log : list) {
					for (XExtension ext : log.getExtensions())
						extensionNames.add( ext.getName() );
					
					for (XEventClassifier classif : log.getClassifiers())
						classifierNames.add( classif.name() );
					
					for (XAttribute att : log.getAttributes().values()) {
						logAttNames.add( att.getKey() );
						for (XAttribute inAtt : att.getAttributes().values())
							logAttNames.add( inAtt.getKey() );
					}
					for (XAttribute att : log.getGlobalEventAttributes()) {
						logGlobAttNames.add( att.getKey() );
						for (XAttribute inAtt : att.getAttributes().values())
							logGlobAttNames.add( inAtt.getKey() );
					}
					for (XAttribute att : log.getGlobalTraceAttributes()) {
						logGlobAttNames.add( att.getKey() );
						for (XAttribute inAtt : att.getAttributes().values())
							logGlobAttNames.add( inAtt.getKey() );
					}
					for (XTrace trace : log) {
						for (XAttribute att : trace.getAttributes().values()) {
							trcAttNames.add( att.getKey() );
							for (XAttribute inAtt : att.getAttributes().values())
								trcAttNames.add( inAtt.getKey() );
						}
					
						for (XEvent event : trace) {
							for (XAttribute att : event.getAttributes().values()) {
								evtAttNames.add( att.getKey() );
								for (XAttribute inAtt : att.getAttributes().values())
									evtAttNames.add( inAtt.getKey() );
							}
						}
					}
				}
				
				for (String ext : extensionNames)
					st.execute( "ALTER TABLE " + tableName + " ADD [log_ext_" + ext + "] varchar(" + VARCHAR_LEN + ") NULL;" );
				
				for (String classif : classifierNames)
					st.execute( "ALTER TABLE " + tableName + " ADD [log_classif_" + classif + "] varchar(" + VARCHAR_LEN + ") NULL;" );
				
				for (String att : logGlobAttNames)
					st.execute( "ALTER TABLE " + tableName + " ADD [log_glob_att_" + att + "] varchar(" + VARCHAR_LEN + ") NULL;" );
				
				for (String att : logAttNames)
					if (!att.equals(XConceptExtension.KEY_NAME))
						st.execute( "ALTER TABLE " + tableName + " ADD [log_att_" + att + "] varchar(" + VARCHAR_LEN + ") NULL;" );
				
				st.execute(
					"ALTER TABLE " + tableName + " ADD "
					+ "trace_id		bigint		NOT NULL, "
					+ "trace_name	varchar(" + VARCHAR_LEN + ")	NULL;"
				);
				
				for (String att : trcAttNames)
					if (!att.equals(XConceptExtension.KEY_NAME))
						st.execute( "ALTER TABLE " + tableName + " ADD [trace_att_" + att + "] varchar(" + VARCHAR_LEN + ");" );
				
				st.execute(
					"ALTER TABLE " + tableName + " ADD "
					+ "event_id		bigint		NOT NULL, "
					+ "event_name	varchar(" + VARCHAR_LEN + ")		NULL, "
					+ "event_transition varchar(" + VARCHAR_LEN + ")	NULL, "
					+ "event_timestamp varchar(" + VARCHAR_LEN + ")		NULL;"
				);
				
				for (String att : evtAttNames)
					if (!att.equals(XConceptExtension.KEY_NAME) && !att.equals(XLifecycleExtension.KEY_TRANSITION) && !att.equals(XTimeExtension.KEY_TIMESTAMP))
						st.execute( "ALTER TABLE " + tableName + " ADD [event_att_" + att + "] varchar(" + VARCHAR_LEN + ") NULL;" );
				
				Map<String, String> headersAndValues = new LinkedHashMap<>();	// Preserving insertion order
						
				long logID = 0;
				for (XLog log : list) {
					String logName = XConceptExtension.instance().extractName(log);
					
					headersAndValues.put("log_id", "'" + logID + "'");
					Commons.populateInsertionMapVarchars(headersAndValues, "log_name", logName, VARCHAR_LEN);
					
					System.out.println("Log " + (logID+1) + " - Converting extensions");
					for (XExtension ext : log.getExtensions())
						Commons.populateInsertionMapVarchars(headersAndValues, "log_ext_"+ext.getName(), ext.getUri().toString(), VARCHAR_LEN);
					
					System.out.println("Log " + (logID+1) + " - Converting classifiers");
					for (XEventClassifier classif : log.getClassifiers())
						Commons.populateInsertionMapVarchars(headersAndValues, "log_classif_"+classif.name(), classif.getDefiningAttributeKeys()[0], VARCHAR_LEN);
					
					System.out.println("Log " + (logID+1) + " - Converting attributes");
					for (XAttribute att : log.getGlobalEventAttributes()) {
						Commons.populateInsertionMapVarchars(headersAndValues, "log_glob_att_"+att.getKey(), att.toString(), VARCHAR_LEN);
						for (XAttribute inAtt : att.getAttributes().values())
							Commons.populateInsertionMapVarchars(headersAndValues, "log_glob_att_"+inAtt.getKey(), inAtt.toString(), VARCHAR_LEN);
					}
					for (XAttribute att : log.getGlobalTraceAttributes()) {
						Commons.populateInsertionMapVarchars(headersAndValues, "log_glob_att_"+att.getKey(), att.toString(), VARCHAR_LEN);
						for (XAttribute inAtt : att.getAttributes().values())
							Commons.populateInsertionMapVarchars(headersAndValues, "log_glob_att_"+inAtt.getKey(), inAtt.toString(), VARCHAR_LEN);
					}
					for (XAttribute att : log.getAttributes().values()) {
						if (!att.getKey().equals(XConceptExtension.KEY_NAME)) {
							Commons.populateInsertionMapVarchars(headersAndValues, "log_att_"+att.getKey(), att.toString(), VARCHAR_LEN);
							for (XAttribute inAtt : att.getAttributes().values())
								if (!inAtt.getKey().equals(XConceptExtension.KEY_NAME))
									Commons.populateInsertionMapVarchars(headersAndValues, "log_att_"+inAtt.getKey(), inAtt.toString(), VARCHAR_LEN);
						}
					}
					
					int logSize = log.size();
					long traceID = 0;
					for (XTrace trace : log) {
						System.out.println("Log " + (logID+1) + " - Converting trace " + (traceID+1) + " of " + logSize);
						
						String traceName = XConceptExtension.instance().extractName(trace);
						
						headersAndValues.put("trace_id", "'" + traceID + "'");
						Commons.populateInsertionMapVarchars(headersAndValues, "trace_name", traceName, VARCHAR_LEN);
						
						for (XAttribute att : trace.getAttributes().values()) {
							if (!att.getKey().equals(XConceptExtension.KEY_NAME)) {
								Commons.populateInsertionMapVarchars(headersAndValues, "trace_att_"+att.getKey(), att.toString(), VARCHAR_LEN);
								for (XAttribute inAtt : att.getAttributes().values())
									if (!inAtt.getKey().equals(XConceptExtension.KEY_NAME))
										Commons.populateInsertionMapVarchars(headersAndValues, "trace_att_"+inAtt.getKey(), inAtt.toString(), VARCHAR_LEN);
							}
						}
						
						long eventID = 0;
						for (XEvent event : trace) {
							String eventName = XConceptExtension.instance().extractName(event);
							
							
							Date eventTimestamp = XTimeExtension.instance().extractTimestamp(event);
							String timestampStr = eventTimestamp.toInstant().toString();							
							String eventTransition = XLifecycleExtension.instance().extractTransition(event);
							
							headersAndValues.put("event_id", "'" + eventID + "'");
							Commons.populateInsertionMapVarchars(headersAndValues, "event_name", eventName, VARCHAR_LEN);
							Commons.populateInsertionMapVarchars(headersAndValues, "event_timestamp", timestampStr, VARCHAR_LEN);
							Commons.populateInsertionMapVarchars(headersAndValues, "event_transition", eventTransition, VARCHAR_LEN);

							for (XAttribute att : event.getAttributes().values()) {
								if (!att.getKey().equals(XConceptExtension.KEY_NAME) && !att.getKey().equals(XLifecycleExtension.KEY_TRANSITION) && !att.getKey().equals(XTimeExtension.KEY_TIMESTAMP)) {
									Commons.populateInsertionMapVarchars(headersAndValues, "event_att_"+att.getKey(), att.toString(), VARCHAR_LEN);
									for (XAttribute inAtt : att.getAttributes().values())
										if (!inAtt.getKey().equals(XConceptExtension.KEY_NAME) && !inAtt.getKey().equals(XLifecycleExtension.KEY_TRANSITION) && !inAtt.getKey().equals(XTimeExtension.KEY_TIMESTAMP))
											Commons.populateInsertionMapVarchars(headersAndValues, "event_att_"+inAtt.getKey(), inAtt.toString(), VARCHAR_LEN);
								}
							}
									
							st.execute(
								"INSERT INTO " + tableName + " ( " + String.join(", ", headersAndValues.keySet()) + " ) "
								+ "VALUES ( " + String.join(", ", headersAndValues.values()) + " );"
							);
							
							for (String key : headersAndValues.keySet())
								if (key.startsWith("[event_att_"))
									headersAndValues.replace(key, null);
							
							eventID++;
						}
						
						for (String key : headersAndValues.keySet())
							if (key.startsWith("[trace_att_"))
								headersAndValues.replace(key, null);
						
						traceID++;
					}
					
					for (String key : headersAndValues.keySet())
						if (key.startsWith("[log_att_") || key.startsWith("[log_glob_att_") || key.startsWith("[log_ext_") || key.startsWith("[log_classif_"))
							headersAndValues.replace(key, null);
					
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
}

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
import java.util.Stack;

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
	private static final int MAX_COLUMN_NAME_LEN = 115;

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
						extensionNames.add( Commons.truncateIfNecessary(ext.getName(), MAX_COLUMN_NAME_LEN) );
					
					for (XEventClassifier classif : log.getClassifiers())
						classifierNames.add( Commons.truncateIfNecessary(classif.name(), MAX_COLUMN_NAME_LEN) );
					
					Stack<XAttribute> attStack = new Stack<>();
					
					for (XAttribute att : log.getAttributes().values())
						if ( !att.getKey().equals(XConceptExtension.KEY_NAME) )
							attStack.push(att);
					
					while (!attStack.isEmpty()) {
						XAttribute att = attStack.pop();
						logAttNames.add( Commons.truncateIfNecessary(att.getKey(), MAX_COLUMN_NAME_LEN) );
						
						/*for (XAttribute inAtt : att.getAttributes().values())
							attStack.push(inAtt);*/
					}
					
					
					for (XAttribute att : log.getGlobalEventAttributes())
						attStack.push(att);
					for (XAttribute att : log.getGlobalTraceAttributes())
						attStack.push(att);
					
					while (!attStack.isEmpty()) {
						XAttribute att = attStack.pop();
						logGlobAttNames.add( Commons.truncateIfNecessary(att.getKey(), MAX_COLUMN_NAME_LEN) );
						
						for (XAttribute inAtt : att.getAttributes().values())
							attStack.push(inAtt);
					}
					
					
					for (XTrace trace : log) {
						for (XAttribute att : trace.getAttributes().values())
							if ( !att.getKey().equals(XConceptExtension.KEY_NAME) )
								attStack.push(att);
						
						while (!attStack.isEmpty()) {
							XAttribute att = attStack.pop();
							trcAttNames.add( Commons.truncateIfNecessary(att.getKey(), MAX_COLUMN_NAME_LEN) );
							
							for (XAttribute inAtt : att.getAttributes().values())
								attStack.push(inAtt);
						}
					
						for (XEvent event : trace) {
							for (XAttribute att : event.getAttributes().values())
								if (!att.getKey().equals(XConceptExtension.KEY_NAME) && !att.getKey().equals(XLifecycleExtension.KEY_TRANSITION) && !att.getKey().equals(XTimeExtension.KEY_TIMESTAMP))
									attStack.push(att);
							
							while (!attStack.isEmpty()) {
								XAttribute att = attStack.pop();
								evtAttNames.add( Commons.truncateIfNecessary(att.getKey(), MAX_COLUMN_NAME_LEN) );
								
								for (XAttribute inAtt : att.getAttributes().values())
									attStack.push(inAtt);
							}
						}
					}
				}
				
				// Adding extension columns
				st.execute( "ALTER TABLE " + tableName + " ADD [log_ext_" + String.join("] VARCHAR(" + VARCHAR_LEN + ") NULL, [log_ext_", extensionNames) + "] VARCHAR(" + VARCHAR_LEN + ") NULL;" );
				
				// Adding classifier columns
				st.execute( "ALTER TABLE " + tableName + " ADD [log_classif_" + String.join("] VARCHAR(" + VARCHAR_LEN + ") NULL, [log_classif_", classifierNames) + "] VARCHAR(" + VARCHAR_LEN + ") NULL;" );
				
				// Adding global attribute columns
				st.execute( "ALTER TABLE " + tableName + " ADD [log_glob_att_" + String.join("] VARCHAR(" + VARCHAR_LEN + ") NULL, [log_glob_att_", logGlobAttNames) + "] VARCHAR(" + VARCHAR_LEN + ") NULL;" );
				
				// Adding log attribute columns
				st.execute( "ALTER TABLE " + tableName + " ADD [log_att_" + String.join("] VARCHAR(" + VARCHAR_LEN + ") NULL, [log_att_", logAttNames) + "] VARCHAR(" + VARCHAR_LEN + ") NULL;" );
				
				st.execute(
					"ALTER TABLE " + tableName + " ADD "
					+ "trace_id		bigint		NOT NULL, "
					+ "trace_name	varchar(" + VARCHAR_LEN + ")	NULL;"
				);
				
				// Adding trace attribute columns
				st.execute( "ALTER TABLE " + tableName + " ADD [trace_att_" + String.join("] VARCHAR(" + VARCHAR_LEN + ") NULL, [trace_att_", trcAttNames) + "] VARCHAR(" + VARCHAR_LEN + ") NULL;" );
				
				st.execute(
					"ALTER TABLE " + tableName + " ADD "
					+ "event_id		bigint		NOT NULL, "
					+ "event_name	varchar(" + VARCHAR_LEN + ")		NULL, "
					+ "event_transition varchar(" + VARCHAR_LEN + ")	NULL, "
					+ "event_timestamp varchar(" + VARCHAR_LEN + ")		NULL;"
				);
				
				// Adding event attribute columns
				st.execute( "ALTER TABLE " + tableName + " ADD [event_att_" + String.join("] VARCHAR(" + VARCHAR_LEN + ") NULL, [event_att_", evtAttNames) + "] VARCHAR(" + VARCHAR_LEN + ") NULL;" );
				
				Map<String, String> headersAndValues = new LinkedHashMap<>();	// Preserving insertion order
						
				long logID = 0;
				for (XLog log : list) {
					headersAndValues.put("log_id", Commons.prepareValueForInsertion(String.valueOf(logID), VARCHAR_LEN));
					
					String logName = XConceptExtension.instance().extractName(log);
					headersAndValues.put("log_name", Commons.prepareValueForInsertion(logName, VARCHAR_LEN));
					
					System.out.println("Log " + (logID+1) + " - Converting extensions");
					for (XExtension ext : log.getExtensions()) {
						String name = "[" + Commons.truncateIfNecessary("log_ext_"+ext.getName(), MAX_COLUMN_NAME_LEN) + "]";
						String val = Commons.prepareValueForInsertion(ext.getUri().toString(), VARCHAR_LEN);
						headersAndValues.put(name, val);
					}
					
					System.out.println("Log " + (logID+1) + " - Converting classifiers");
					for (XEventClassifier classif : log.getClassifiers()) {
						String name = "[" + Commons.truncateIfNecessary("log_classif_"+classif.name(), MAX_COLUMN_NAME_LEN) + "]";
						String val = Commons.prepareValueForInsertion(classif.getDefiningAttributeKeys()[0], VARCHAR_LEN);
						headersAndValues.put(name, val);
					}
					
					
					System.out.println("Log " + (logID+1) + " - Converting attributes");
					Stack<XAttribute> attStack = new Stack<>();
					
					for (XAttribute att : log.getGlobalEventAttributes())
						attStack.push(att);
					for (XAttribute att : log.getGlobalTraceAttributes())
						attStack.push(att);
					
					while (!attStack.isEmpty()) {
						XAttribute att = attStack.pop();
						String name = "[" + Commons.truncateIfNecessary("log_glob_att_"+att.getKey(), MAX_COLUMN_NAME_LEN) + "]";
						String val = Commons.prepareValueForInsertion(att.toString(), VARCHAR_LEN);
						headersAndValues.put(name, val);
						
						for (XAttribute inAtt : att.getAttributes().values())
							attStack.push(inAtt);
					}
					
					
					for (XAttribute att : log.getAttributes().values())
						if ( !att.getKey().equals(XConceptExtension.KEY_NAME) )
							attStack.push(att);
					
					while (!attStack.isEmpty()) {
						XAttribute att = attStack.pop();
						String name = "[" + Commons.truncateIfNecessary("log_att_"+att.getKey(), MAX_COLUMN_NAME_LEN) + "]";
						String val = Commons.prepareValueForInsertion(att.toString(), VARCHAR_LEN);
						headersAndValues.put(name, val);
						
						/*for (XAttribute inAtt : att.getAttributes().values())
							attStack.push(inAtt);*/
					}
					
					
					int logSize = log.size();
					long traceID = 0;
					for (XTrace trace : log) {
						System.out.println("Log " + (logID+1) + " - Converting trace " + (traceID+1) + " of " + logSize);
						
						headersAndValues.put("trace_id", Commons.prepareValueForInsertion(String.valueOf(traceID), VARCHAR_LEN));
						
						String traceName = XConceptExtension.instance().extractName(trace);
						headersAndValues.put("trace_name", Commons.prepareValueForInsertion(traceName, VARCHAR_LEN));
						
						for (XAttribute att : trace.getAttributes().values())
							if ( !att.getKey().equals(XConceptExtension.KEY_NAME) )
								attStack.push(att);
						
						while (!attStack.isEmpty()) {
							XAttribute att = attStack.pop();
							String name = "[" + Commons.truncateIfNecessary("trace_att_"+att.getKey(), MAX_COLUMN_NAME_LEN) + "]";
							String val = Commons.prepareValueForInsertion(att.toString(), VARCHAR_LEN);
							headersAndValues.put(name, val);
							
							for (XAttribute inAtt : att.getAttributes().values())
								attStack.push(inAtt);
						}
						
						long eventID = 0;
						for (XEvent event : trace) {
							String eventName = XConceptExtension.instance().extractName(event);
							
							Date eventTimestamp = XTimeExtension.instance().extractTimestamp(event);
							String timestampStr = eventTimestamp.toInstant().toString();							
							String eventTransition = XLifecycleExtension.instance().extractTransition(event);
							
							headersAndValues.put("event_id", Commons.prepareValueForInsertion(String.valueOf(eventID), VARCHAR_LEN));
							headersAndValues.put("event_name", Commons.prepareValueForInsertion(eventName, VARCHAR_LEN));
							headersAndValues.put("event_timestamp", Commons.prepareValueForInsertion(timestampStr, VARCHAR_LEN));
							headersAndValues.put("event_transition", Commons.prepareValueForInsertion(eventTransition, VARCHAR_LEN));

							for (XAttribute att : event.getAttributes().values())
								if (!att.getKey().equals(XConceptExtension.KEY_NAME) && !att.getKey().equals(XLifecycleExtension.KEY_TRANSITION) && !att.getKey().equals(XTimeExtension.KEY_TIMESTAMP))
									attStack.push(att);
							
							while (!attStack.isEmpty()) {
								XAttribute att = attStack.pop();
								String name = "[" + Commons.truncateIfNecessary("event_att_"+att.getKey(), MAX_COLUMN_NAME_LEN) + "]";
								String val = Commons.prepareValueForInsertion(att.toString(), VARCHAR_LEN);
								headersAndValues.put(name, val);
								
								for (XAttribute inAtt : att.getAttributes().values())
									attStack.push(inAtt);
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

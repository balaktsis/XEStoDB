package sql_server;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

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
import us.hebi.matlab.mat.format.Mat5;
import us.hebi.matlab.mat.types.MatFile;
import us.hebi.matlab.mat.types.MatlabType;
import us.hebi.matlab.mat.types.Matrix;
import us.hebi.matlab.mat.types.Sink;
import us.hebi.matlab.mat.types.Sinks;

public class XesToMonolithic {
	
	private static final String USER = "sa";
	private static final String PWD = "Riva96_shared_db";
	private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	
	private enum Scope {NONE, EVENT, TRACE};
	
	private static final int MAX_COLUMN_NAME_LEN = 115;
	
	public static void main(String[] args) throws IOException {
		System.out.print("Enter name of the database to populate: ");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String dbName = reader.readLine();
    	String dbUrl = "jdbc:sqlserver://localhost:1433;databaseName=" + dbName + ";encrypt=true;trustServerCertificate=true;";
    	
		File logFile = Commons.selectLogFile();	//new File(System.getProperty("user.dir"), "prova.xes");
		if (logFile == null) return;
				
		System.out.println("Parsing XES file ... ");
		List<XLog> list = Commons.convertToXlog(logFile);
		System.out.println("Complete!");
		
		try (Connection conn = Commons.getConnection(USER, PWD, dbUrl, DRIVER_CLASS)) {
			
			List<Long> elapsedTimeList = new LinkedList<>();
			List<List<Long>> evtInsTimeList = new LinkedList<>();
			
			for (int i=0; i<1; i++) {
				
				List<Long> evtInsTimes = new LinkedList<>();
				long startTime = System.currentTimeMillis();
				
				try (Statement st = conn.createStatement()) {
					// Setting recovery mode from Full to Simple to avoid transaction log overflow
					st.execute("ALTER DATABASE " + dbName + " SET RECOVERY SIMPLE;");
					// Emptying transaction log
					st.execute("DBCC SHRINKFILE (" + dbName + "_log, 1);");
					
					// Initialising the monolithic table
					st.execute(
						"DROP TABLE IF EXISTS log;"
						+ "CREATE TABLE log ("
							+ "log_id 		bigint		NOT NULL,"
							+ "log_name 	varchar(250)	NULL"
						+ ");"
					);
					
					System.out.println("Try no " + (i+1) + "\tInserting columns into the log table");
					insertLogColumns(st, list);
					
					Map<String, String> headersAndValues = new LinkedHashMap<>();	// Preserving insertion order
					
					startTime = System.currentTimeMillis();
					
					long logCtr = 0;
					for (XLog log : list) {
						String logName = XConceptExtension.instance().extractName(log);
						putLog(st, logCtr, logName, headersAndValues);
						
						System.out.println("Try no " + (i+1) + "\tLog " + (logCtr+1) + "\tConverting extensions");
						putExtensions(st, log.getExtensions(), headersAndValues);
						
						System.out.println("Try no " + (i+1) + "\tLog " + (logCtr+1) + "\tConverting classifiers");
						putClassifiers(st, log.getClassifiers(), headersAndValues);
						
						/* Removing log attributes insertion for measuring performance
						System.out.println("Try no " + (i+1) + "\tLog " + (logCtr+1) + "\tConverting attributes");
						putLogAttributes(st, log.getAttributes().values().stream().filter(att -> !att.getKey().equals(XConceptExtension.KEY_NAME)).collect(Collectors.toList()), headersAndValues, Scope.NONE);
						putLogAttributes(st, log.getGlobalTraceAttributes(), headersAndValues, Scope.TRACE);
						putLogAttributes(st, log.getGlobalEventAttributes(), headersAndValues, Scope.EVENT);
						*/
						long traceCtr = 0;
						for (XTrace trace : log) {
							System.out.println("Try no " + (i+1) + "\tLog " + (logCtr+1) + "\tConverting trace " + (traceCtr+1) + " of " + log.size());
							
							String traceName = XConceptExtension.instance().extractName(trace);
							putTrace(st, traceCtr, traceName, headersAndValues);
							putTraceAttributes(st, trace.getAttributes().values().stream().filter(att -> !att.getKey().equals(XConceptExtension.KEY_NAME)).collect(Collectors.toList()), headersAndValues);
							
							long eventCtr = 0;
							for (XEvent event : trace) {
								long evtInsStart = System.currentTimeMillis();
								
								String eventName = XConceptExtension.instance().extractName(event);
								String eventTransition = XLifecycleExtension.instance().extractTransition(event);
								Date eventTimestamp = XTimeExtension.instance().extractTimestamp(event);							
								
								putEvent(st, eventCtr, eventName, eventTransition, eventTimestamp, headersAndValues);
								putEventAttributes(
									st, 
									event.getAttributes().values().stream()
																	.filter(att -> !att.getKey().equals(XConceptExtension.KEY_NAME)
																			&& !att.getKey().equals(XLifecycleExtension.KEY_TRANSITION)
																			&& !att.getKey().equals(XTimeExtension.KEY_TIMESTAMP))
																	.collect(Collectors.toList()), 
									headersAndValues
								);
								
								// Inserting all of the values put inside the map
								st.execute(
									"INSERT INTO log ( " + String.join(", ", headersAndValues.keySet()) + " ) "
									+ "VALUES ( " + String.join(", ", headersAndValues.values()) + " );"
								);
								
								for (String key : headersAndValues.keySet())
									if (key.startsWith("[event_att_"))
										headersAndValues.replace(key, null);
								
								long evtInsEnd = System.currentTimeMillis();
								evtInsTimes.add(evtInsEnd-evtInsStart);
								
								eventCtr++;
							}
							
							for (String key : headersAndValues.keySet())
								if (key.startsWith("[trace_att_"))
									headersAndValues.replace(key, null);
							
							traceCtr++;
						}
						
						for (String key : headersAndValues.keySet())
							if (key.startsWith("[log_att_") || key.startsWith("[glob_trc_att_") || key.startsWith("[glob_evt_att_") || key.startsWith("[log_ext_") || key.startsWith("[log_classif_"))
								headersAndValues.replace(key, null);
						
						logCtr++;
					}
					
					// Emptying transaction log
					st.execute("DBCC SHRINKFILE (" + dbName + "_log, 1);");
				}
				
				long endTime = System.currentTimeMillis();
				long elapsedTime = endTime - startTime;
				System.out.println("Succesfully concluded in " + ((double) elapsedTime / 1000) + " seconds\n");
				
				evtInsTimeList.add(evtInsTimes);
				elapsedTimeList.add(elapsedTime);
			}
			
			System.out.println("Elapsed times (millis):");
			for (long time : elapsedTimeList)
				System.out.println(time);
			
			/*
			System.out.print("Writing event insertion times to file... ");
			File csvOutputFile = new File("mono_eventInsertionTimes.csv");
			try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
				for (List<Long> times : evtInsTimeList)
					pw.println( times.stream().map(l -> String.valueOf(l)).collect(Collectors.joining(",")) );
				
				System.out.println("Complete!");
			}
			*/
			/*
			System.out.print("Writing mean of event insertion times to file... ");
			File txtOutputFile = new File("mono_eventInsertionMeans.txt");
			try (PrintWriter pw = new PrintWriter(txtOutputFile)) {
				for (int i=0; i<evtInsTimeList.get(0).size(); i++) {
					//if(i%1000==0) System.out.println(i);
					List<Long> evtInsTimePerPos = new ArrayList<>();
					
					for (List<Long> times : evtInsTimeList)
						evtInsTimePerPos.add(times.get(i));
					
					evtInsTimePerPos.remove(Collections.max(evtInsTimePerPos));
					evtInsTimePerPos.remove(Collections.min(evtInsTimePerPos));
					
					pw.println(evtInsTimePerPos.stream().mapToLong(val -> val).average().getAsDouble());
				}
				
				System.out.println("Complete!");
			}
			*/
			
			System.out.print("Writing event insertion times to MatLab file... ");
			
			Matrix evtInsTimesMatrix = Mat5.newMatrix(evtInsTimeList.get(0).size(), evtInsTimeList.size(), MatlabType.Int32);
			for (int col=0; col < evtInsTimeList.size(); col++) {
				int row = 0;
				Iterator<Long> it =  evtInsTimeList.get(col).iterator();
				while (it.hasNext()) {
					evtInsTimesMatrix.setLong(row, col, it.next());
					row++;
				}
			}
			
			File matOutputFile = new File("event_insertion_times.mat");
			if (!matOutputFile.exists()) {
				
				MatFile matFile = Mat5.newMatFile().addArray(dbName, evtInsTimesMatrix);
				try(Sink sink = Sinks.newStreamingFile("event_insertion_times.mat")) {
				    matFile.writeTo(sink);
				}
			} else {
				try (Sink sink = Sinks.newStreamingFile(matOutputFile, true)) {
		            Mat5.newWriter(sink).writeArray(dbName, evtInsTimesMatrix);
		        }
			}
			
			System.out.println("Complete!");
			
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private static void insertLogColumns(Statement stmt, List<XLog> list) throws SQLException {
		Set<String> extensionNames = new LinkedHashSet<>();
		Set<String> classifierNames = new LinkedHashSet<>();
		Set<String> globTraceAttNames = new LinkedHashSet<>();
		Set<String> globEventAttNames = new LinkedHashSet<>();
		//Set<String> logAttNames = new LinkedHashSet<>();
		Set<String> trcAttNames = new LinkedHashSet<>();
		Set<String> evtAttNames = new LinkedHashSet<>();
		
		for (XLog log : list) {
			for (XExtension ext : log.getExtensions())
				extensionNames.add( Commons.truncateIfNecessary(ext.getName(), MAX_COLUMN_NAME_LEN) );
			
			for (XEventClassifier classif : log.getClassifiers())
				classifierNames.add( Commons.truncateIfNecessary(classif.name(), MAX_COLUMN_NAME_LEN) );
			
			Stack<XAttribute> attStack = new Stack<>();
			
			/* Removing log attributes insertion for measuring performance
			for (XAttribute att : log.getAttributes().values())
				if ( !att.getKey().equals(XConceptExtension.KEY_NAME) )
					attStack.push(att);
			
			while (!attStack.isEmpty()) {
				XAttribute att = attStack.pop();
				logAttNames.add( Commons.truncateIfNecessary(att.getKey(), MAX_COLUMN_NAME_LEN) );
				
				// REMOVING NESTED ATTRIBUTES !!! Because they are too much to fit in a single table (max 1024 cols)
				//for (XAttribute inAtt : att.getAttributes().values())
				//	attStack.push(inAtt);
			}
			
			for (XAttribute att : log.getGlobalTraceAttributes())
				attStack.push(att);
			
			while (!attStack.isEmpty()) {
				XAttribute att = attStack.pop();
				globTraceAttNames.add( Commons.truncateIfNecessary(att.getKey(), MAX_COLUMN_NAME_LEN) );
				
				// REMOVING NESTED ATTRIBUTES !!! Because they are too much to fit in a single table (max 1024 cols)
				//for (XAttribute inAtt : att.getAttributes().values())
				//	attStack.push(inAtt);
			}
			
			
			for (XAttribute att : log.getGlobalEventAttributes())
				attStack.push(att);
			
			while (!attStack.isEmpty()) {
				XAttribute att = attStack.pop();
				globEventAttNames.add( Commons.truncateIfNecessary(att.getKey(), MAX_COLUMN_NAME_LEN) );
				
				// REMOVING NESTED ATTRIBUTES !!! Because they are too much to fit in a single table (max 1024 cols)
				//for (XAttribute inAtt : att.getAttributes().values())
				//	attStack.push(inAtt);
			}
			*/
			
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
		if (!extensionNames.isEmpty())
			stmt.execute( "ALTER TABLE log ADD [log_ext_" + String.join("] VARCHAR(250) NULL, [log_ext_", extensionNames) + "] VARCHAR(250) NULL;" );
		
		// Adding classifier columns
		if (!classifierNames.isEmpty())
			stmt.execute( "ALTER TABLE log ADD [log_classif_" + String.join("] VARCHAR(250) NULL, [log_classif_", classifierNames) + "] VARCHAR(250) NULL;" );
		
		// Adding global trace attribute columns
		if (!globTraceAttNames.isEmpty())
			stmt.execute( "ALTER TABLE log ADD [glob_trc_att_" + String.join("] VARCHAR(250) NULL, [glob_trc_att_", globTraceAttNames) + "] VARCHAR(250) NULL;" );
		
		// Adding global event attribute columns
		if (!globEventAttNames.isEmpty())
			stmt.execute( "ALTER TABLE log ADD [glob_evt_att_" + String.join("] VARCHAR(250) NULL, [glob_evt_att_", globEventAttNames) + "] VARCHAR(250) NULL;" );
		
		/* Removing log attributes insertion for measuring performance
		// Adding log attribute columns
		if (!logAttNames.isEmpty())
			stmt.execute( "ALTER TABLE log ADD [log_att_" + String.join("] VARCHAR(250) NULL, [log_att_", logAttNames) + "] VARCHAR(250) NULL;" );
		*/
		
		stmt.execute(
			"ALTER TABLE log ADD "
			+ "trace_id		bigint		NOT NULL, "
			+ "trace_name	varchar(250)	NULL;"
		);
		
		// Adding trace attribute columns
		if (!trcAttNames.isEmpty())
			stmt.execute( "ALTER TABLE log ADD [trace_att_" + String.join("] VARCHAR(250) NULL, [trace_att_", trcAttNames) + "] VARCHAR(250) NULL;" );
		
		stmt.execute(
			"ALTER TABLE log ADD "
			+ "event_id		bigint		NOT NULL, "
			+ "event_name	varchar(250)		NULL, "
			+ "event_transition varchar(50)	NULL, "
			+ "event_timestamp datetime2(3)		NULL;"
		);
		
		// Adding event attribute columns
		if (!evtAttNames.isEmpty())
			stmt.execute( "ALTER TABLE log ADD [event_att_" + String.join("] VARCHAR(250) NULL, [event_att_", evtAttNames) + "] VARCHAR(250) NULL;" );
		
	}
	
	private static void putLog(Statement stmt, long logId, String logName, Map<String, String> headsAndVals) {
		headsAndVals.put("log_id", String.valueOf(logId));
		headsAndVals.put("log_name", Commons.prepareValueForInsertion(logName, 250));
	}
	
	private static void putExtensions(Statement stmt, Set<XExtension> extensions, Map<String, String> headsAndVals) {
		for (XExtension ext : extensions) {
			String name = "[" + Commons.truncateIfNecessary("log_ext_"+ext.getName(), MAX_COLUMN_NAME_LEN) + "]";
			String val = Commons.prepareValueForInsertion(ext.getUri().toString(), 250);
			headsAndVals.put(name, val);
		}
	}
	
	private static void putClassifiers(Statement stmt, List<XEventClassifier> classifiers, Map<String, String> headsAndVals) {
		for (XEventClassifier classif : classifiers) {
			String name = "[" + Commons.truncateIfNecessary("log_classif_"+classif.name(), MAX_COLUMN_NAME_LEN) + "]";
			String val = Commons.prepareValueForInsertion(String.join(", ", classif.getDefiningAttributeKeys()), 250);
			headsAndVals.put(name, val);
		}
	}
	
	private static void putLogAttributes(Statement stmt, Collection<XAttribute> attributes, Map<String, String> headsAndVals, Scope scope) {
		String prefix = "";
		switch (scope) {
		case EVENT:
			prefix = "glob_evt_att_";
			break;
		case TRACE:
			prefix = "glob_trc_att_";
			break;
		case NONE:
			prefix = "log_att_";
			break;
		}
		
		Stack<XAttribute> attStack = new Stack<>();
		
		for (XAttribute att : attributes)
			attStack.push(att);
		
		while (!attStack.isEmpty()) {
			XAttribute att = attStack.pop();
			String name = "[" + Commons.truncateIfNecessary(prefix+att.getKey(), MAX_COLUMN_NAME_LEN) + "]";
			String val = Commons.prepareValueForInsertion(att.toString(), 250);
			headsAndVals.put(name, val);
			
			// REMOVING NESTED ATTRIBUTES !!! Because they are too much to fit in a single table (max 1024 cols)
			//for (XAttribute inAtt : att.getAttributes().values())
			//	attStack.push(inAtt);
		}
	}
	
	private static void putTrace(Statement stmt, long traceId, String traceName, Map<String, String> headsAndVals) {
		headsAndVals.put("trace_id", String.valueOf(traceId));
		headsAndVals.put("trace_name", Commons.prepareValueForInsertion(traceName, 250));
	}
	
	private static void putTraceAttributes(Statement stmt, Collection<XAttribute> attributes, Map<String, String> headsAndVals) {
		Stack<XAttribute> attStack = new Stack<>();
		
		for (XAttribute att : attributes)
			attStack.push(att);
		
		while (!attStack.isEmpty()) {
			XAttribute att = attStack.pop();
			String name = "[" + Commons.truncateIfNecessary("trace_att_"+att.getKey(), MAX_COLUMN_NAME_LEN) + "]";
			String val = Commons.prepareValueForInsertion(att.toString(), 250);
			headsAndVals.put(name, val);
			
			for (XAttribute inAtt : att.getAttributes().values())
				attStack.push(inAtt);
		}
	}
	
	private static void putEvent(Statement stmt, long eventId, String eventName, String eventTransition, Date eventTimestamp, Map<String, String> headsAndVals) {
		headsAndVals.put("event_id", String.valueOf(eventId));
		headsAndVals.put("event_name", Commons.prepareValueForInsertion(eventName, 250));
		headsAndVals.put("event_transition", Commons.prepareValueForInsertion(eventTransition, 50));
		headsAndVals.put("event_timestamp", Commons.prepareValueForInsertion(eventTimestamp.toInstant().toString(), 250));
	}
	
	private static void putEventAttributes(Statement stmt, Collection<XAttribute> attributes, Map<String, String> headsAndVals) {
		Stack<XAttribute> attStack = new Stack<>();
		
		for (XAttribute att : attributes)
			attStack.push(att);
		
		while (!attStack.isEmpty()) {
			XAttribute att = attStack.pop();
			String name = "[" + Commons.truncateIfNecessary("event_att_"+att.getKey(), MAX_COLUMN_NAME_LEN) + "]";
			String val = Commons.prepareValueForInsertion(att.toString(), 250);
			headsAndVals.put(name, val);
			
			for (XAttribute inAtt : att.getAttributes().values())
				attStack.push(inAtt);
		}
	}
}

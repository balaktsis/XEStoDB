package scripts.sql_server;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
import us.hebi.matlab.mat.format.Mat5;
import us.hebi.matlab.mat.types.MatFile;
import us.hebi.matlab.mat.types.MatlabType;
import us.hebi.matlab.mat.types.Matrix;
import us.hebi.matlab.mat.types.Sink;
import us.hebi.matlab.mat.types.Sinks;

public class XesToDbxes {

	private static final String USER = "sa";
	private static final String PWD = "Riva96_shared_db";
	private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	
	private enum Scope {NONE, EVENT, TRACE};
	
	public static void main(String[] args) throws IOException {
		System.out.print("Enter name of the database to populate: ");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String dbName = "dbxes_" + reader.readLine();
    	String dbUrl = "jdbc:sqlserver://localhost:1433;databaseName=" + dbName + ";encrypt=true;trustServerCertificate=true;";
    	
    	File logFile = Commons.selectLogFile();	// new File(System.getProperty("user.dir"), "prova.xes");
		if (logFile == null) return;
		
		System.out.println("Parsing XES file ... ");
		List<XLog> list = Commons.convertToXlog(logFile);
		System.out.println("Complete!");
		
		try (Connection conn = Commons.getConnection(USER, PWD, dbUrl, DRIVER_CLASS)) {
			
			List<Long> elapsedTimeList = new LinkedList<>();
			List<List<Long>> evtInsTimeList = new LinkedList<>();
			
			for (int i=0; i<5; i++) {
				
				List<Long> evtInsTimes = new LinkedList<>();
				long startTime = System.currentTimeMillis();
				
				try (Statement st = conn.createStatement()) {
					// Setting recovery mode from Full to Simple to avoid transaction log overflow
					st.execute("ALTER DATABASE " + dbName + " SET RECOVERY SIMPLE;");
					// Clearing all data previously contained in the database
					st.execute("EXEC sp_MSForEachTable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL';");
					st.execute("EXEC sp_MSForEachTable 'DELETE FROM ?';");
					st.execute("EXEC sp_MSForEachTable 'ALTER TABLE ? CHECK CONSTRAINT ALL';");
					// Emptying transaction log
					st.execute("DBCC SHRINKFILE (" + dbName + "_log, 1);");
					
					long logCtr, traceCtr, eventCtr;
					ResultSet resultingId;
					
					startTime = System.currentTimeMillis();
					
					for (XLog log : list) {
						resultingId = st.executeQuery("SELECT COALESCE( (SELECT MAX(id) FROM log)+1 , 0 );");
						resultingId.next();
						logCtr = resultingId.getLong(1);
						
						insertLog(st, logCtr);
						
						System.out.println("Try no " + (i+1) + "\tLog " + (logCtr+1) + "\tConverting extensions");
						insertExtensions(st, logCtr, log.getExtensions());
						
						System.out.println("Try no " + (i+1) + "\tLog " + (logCtr+1) + "\tConverting classifiers");
						insertLogClassifiers(st, logCtr, log.getClassifiers());
						
						for (XTrace trace : log) {
							resultingId = st.executeQuery("SELECT COALESCE( (SELECT MAX(id) FROM trace)+1 , 0 );");
							resultingId.next();
							traceCtr = resultingId.getLong(1);
							
							System.out.println("Try no " + (i+1) + "\tLog " + (logCtr+1) + "\tConverting trace " + (traceCtr+1) + " of " + log.size());
							
							insertTrace(st, traceCtr, logCtr);
							insertTraceAttributes(st, traceCtr, trace.getAttributes().values(), null);
							
							for (XEvent event : trace) {
								long evtInsStart = System.currentTimeMillis();
								
								resultingId = st.executeQuery("SELECT COALESCE( (SELECT MAX(id) FROM event)+1 , 0 );");
								resultingId.next();
								eventCtr = resultingId.getLong(1);
								
								insertEvent(st, eventCtr, traceCtr);
								insertEventAttributes(st, eventCtr, event.getAttributes().values(), null);
								
								long evtInsEnd = System.currentTimeMillis();
								evtInsTimes.add(evtInsEnd-evtInsStart);
							}
						}
						
						/* Removing log attributes insertion for measuring performance
						System.out.println("Try no " + (i+1) + "\tLog " + (logCtr+1) + "\tConverting log attributes");
						insertLogAttributes(st, logCtr, log.getAttributes().values(), null, Scope.NONE);
						insertLogAttributes(st, logCtr, log.getGlobalTraceAttributes(), null, Scope.TRACE);
						insertLogAttributes(st, logCtr, log.getGlobalEventAttributes(), null, Scope.EVENT);
						*/
						// Inserting only log name
						insertLogAttributes(st, logCtr, log.getAttributes().values().stream().filter(att -> att.getKey().equals(XConceptExtension.KEY_NAME)).collect(Collectors.toList()), null, Scope.NONE);
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
	
	private static void insertLog(Statement stmt, long logId) throws SQLException {		
		stmt.execute(
			"INSERT INTO log ( id ) "
			+ "VALUES ( " + logId + " );"
		);
	}
	
	private static void insertExtensions(Statement stmt, long logId, Set<XExtension> extSet) throws SQLException {
		for (XExtension ext : extSet) {
			String name = Commons.prepareValueForInsertion(ext.getName(), 50);
			String prefix = Commons.prepareValueForInsertion(ext.getPrefix(), 50);
			String uri = Commons.prepareValueForInsertion(ext.getUri().toString(), 250);
			
			stmt.execute(
				"DECLARE @ext_id BIGINT = NULL;"
				
				+ "SELECT @ext_id = id FROM extension "
				+ "WHERE name" + Commons.selectPredicateCaseSens(name) + name
					+ " AND prefix" + Commons.selectPredicateCaseSens(prefix) + prefix
					+ " AND uri" + Commons.selectPredicateCaseSens(uri) + uri + ";"
				
				+ "IF @ext_id IS NULL "
					+ "SET @ext_id = COALESCE( (SELECT MAX(id) FROM extension)+1 , 0 );"
					
				+ "INSERT INTO extension ( id, name, prefix, uri ) "
				+ "VALUES ( @ext_id, " + name + ", " + prefix + ", " + uri + " );"
				
				+ "INSERT INTO log_has_ext ( log_id, ext_id ) "
				+ "VALUES ( " + logId + ", @ext_id );"
			);
		}
	}
	
	private static void insertLogClassifiers(Statement stmt, long logId, List<XEventClassifier> classifierList) throws SQLException {		
		for (XEventClassifier classif : classifierList) {
			String name = Commons.prepareValueForInsertion(classif.name(), 50);
			String keys = Commons.prepareValueForInsertion(String.join(", ", classif.getDefiningAttributeKeys()), 250);
			
			stmt.execute(
				"DECLARE @classif_id BIGINT = NULL;"
				
				+ "SELECT @classif_id = id FROM classifier "
				+ "WHERE name" + Commons.selectPredicateCaseSens(name) + name
					+ " AND keys" + Commons.selectPredicateCaseSens(keys) + keys
				
				+ "IF @classif_id IS NULL "
					+ "SET @classif_id = COALESCE( (SELECT MAX(id) FROM classifier)+1 , 0 );"
				
				+ "INSERT INTO classifier ( id, name, keys ) "
				+ "VALUES ( @classif_id, " + name + ", " + keys + " );"
				
				+ "INSERT INTO log_has_classifier ( log_id, classifier_id ) "
				+ "VALUES ( " + logId + ", @classif_id );"
			);
		}
	}

	private static void insertLogAttributes(Statement stmt, long logId, Collection<XAttribute> attributes, Long parentId, Scope scope) throws SQLException {
		for (XAttribute att : attributes) {
			long attributeId = insertAttribute(stmt, att, parentId);
			
			String scopeStr = scope.equals(Scope.NONE) ? null : scope.toString().toLowerCase();
			scopeStr = Commons.prepareValueForInsertion(scopeStr, 50);
			
			stmt.execute(
				"IF NOT EXISTS (SELECT 1 FROM log_has_attribute "
								+ "WHERE log_id = " + logId
								+ " AND attr_id = " + attributeId
							+ ") "
					+ "INSERT INTO log_has_attribute ( log_id, attr_id, [scope] )"
					+ "VALUES ( " + logId + ", " + attributeId + ", " + scopeStr + " );"
			);
			
			// Recursion over nested attributes
			insertLogAttributes(stmt, logId, att.getAttributes().values(), attributeId, scope);
		}
	}
		
	private static void insertTrace(Statement stmt, long traceId, long logId) throws SQLException {		
		stmt.execute(
			"INSERT INTO trace ( id ) "
			+ "VALUES ( " + traceId + " );"
			
			+ "INSERT INTO log_has_trace ( log_id, trace_id ) "
			+ "VALUES ( " + logId + ", " + traceId + " );"
		);
	}
	
	private static void insertTraceAttributes(Statement stmt, long traceId, Collection<XAttribute> attributes, Long parentId) throws SQLException {
		
		for (XAttribute att : attributes) {
			long attributeId = insertAttribute(stmt, att, parentId);
									
			stmt.execute(
				"INSERT INTO trace_has_attribute ( trace_id, attr_id )"
				+ "VALUES ( " + traceId + ", " + attributeId + " );"
			);
			
			// Recursion over nested attributes
			insertTraceAttributes(stmt, traceId, att.getAttributes().values(), attributeId);
		}
	}
	
	private static void insertEvent(Statement stmt, long eventId, long traceId) throws SQLException {
		stmt.execute(
			"INSERT INTO event ( id, event_coll_id ) "
			+ "VALUES ( " + eventId + ", NULL );"
			
			+ "INSERT INTO trace_has_event ( trace_id, event_id ) "
			+ "VALUES ( " + traceId + ", " + eventId + " );"
		);
	}
	
	private static void insertEventAttributes(Statement stmt, long eventId, Collection<XAttribute> attributes, Long parentId) throws SQLException {
		
		for (XAttribute att : attributes) {
			long attributeId = insertAttribute(stmt, att, parentId);
									
			stmt.execute(
				"INSERT INTO event_has_attribute ( event_id, attr_id )"
				+ "VALUES ( " + eventId + ", " + attributeId + " );"
			);
			
			// Recursion over nested attributes
			insertEventAttributes(stmt, eventId, att.getAttributes().values(), attributeId);
		}
	}

	private static long insertAttribute(Statement stmt, XAttribute att, Long parentId) throws SQLException {
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
			value = Commons.prepareValueForInsertion(dateAtt.getValue().toInstant().toString(), 250);
		} else
			value = Commons.prepareValueForInsertion(att.toString(), 250);

		String extIdStr;
		if (att.getExtension() != null) {
			String extName = Commons.prepareValueForInsertion(att.getExtension().getName(), 50);
			String extPrefix = Commons.prepareValueForInsertion(att.getExtension().getPrefix(), 50);
			String extUri = Commons.prepareValueForInsertion(att.getExtension().getUri().toString(), 250);
			
			ResultSet extId = stmt.executeQuery(
				"SELECT id FROM extension "
				+ "WHERE name" + Commons.selectPredicateCaseSens(extName) + extName
					+ " AND prefix" + Commons.selectPredicateCaseSens(extPrefix) + extPrefix
					+ " AND uri" + Commons.selectPredicateCaseSens(extUri) + extUri + ";"
			);
			
			extIdStr = extId.next() ? extId.getString(1) : "NULL";
		} else {
			extIdStr = "NULL";
		}
		
		String parentIdStr = parentId == null ? "NULL" : String.valueOf(parentId);
		
		ResultSet attributeIdQuery = stmt.executeQuery(
			"DECLARE @att_id BIGINT = NULL, "
				+ "@next_id BIGINT = COALESCE( (SELECT MAX(id) FROM attribute)+1 , 0 );"
				
			+ "SELECT TOP(1) @att_id = id FROM attribute "
			+ "WHERE [type]" + Commons.selectPredicateCaseSens(type) + type
				+ " AND [key]" + Commons.selectPredicateCaseSens(key) + key
				+ " AND value" + Commons.selectPredicateCaseSens(value) + value
				+ " AND ext_id" + Commons.selectPredicate(extIdStr) + extIdStr
				+ " AND parent_id" + Commons.selectPredicate(parentIdStr) + parentIdStr + ";"
			
			+ "IF @att_id IS NULL "
				+ "INSERT INTO attribute ( id, [type], [key], value, ext_id, parent_id ) "
				+ "OUTPUT INSERTED.id "
				+ "VALUES ( @next_id, " + type + ", " + key + ", " + value + ", " + extIdStr + ", " + parentIdStr + " ) "
			+ "ELSE "
				+ "SELECT @att_id;"
		);
		
		attributeIdQuery.next();
		return attributeIdQuery.getLong(1);
	}
}

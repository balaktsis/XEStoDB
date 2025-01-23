package sql_server;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
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
import org.deckfour.xes.extension.XExtensionManager;

// Register the time extension

// Add Maven dependency to pom.xml if using Maven:
/*
<dependency>
    <groupId>org.deckfour</groupId>
    <artifactId>xes</artifactId>
    <version>2.0.5</version>
</dependency>
*/

import commons.Commons;
import us.hebi.matlab.mat.format.Mat5;
import us.hebi.matlab.mat.types.MatFile;
import us.hebi.matlab.mat.types.MatlabType;
import us.hebi.matlab.mat.types.Matrix;
import us.hebi.matlab.mat.types.Sink;
import us.hebi.matlab.mat.types.Sinks;

public class XesToRxesPlus {

	private static final String USER = "postgres";
	private static final String PWD = "postgres";
	private static final String DRIVER_CLASS = "org.postgresql.Driver";

	private enum Scope {NONE, EVENT, TRACE};

	public static void main(String[] args) throws IOException {
		System.out.print("Enter name of the database to populate: ");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String dbName = reader.readLine();
		String dbUrl = "jdbc:postgresql://localhost:5432/" + dbName;

		XExtensionManager.instance().register(XTimeExtension.instance());


		File logFile = Commons.selectLogFile();	// new File(System.getProperty("user.dir"), "prova.xes");
		if (logFile == null) return;

		System.out.println("Parsing XES file ... ");
		List<XLog> list = Commons.convertToXlog(logFile);
		System.out.println("Complete!");

		try (Connection conn = Commons.getConnection(USER, PWD, dbUrl, DRIVER_CLASS)) {

			List<Long> elapsedTimeList = new LinkedList<>();
			List<List<Long>> evtInsTimeList = new LinkedList<>();

			for (int i = 0; i < 1; i++) {

				List<Long> evtInsTimes = new LinkedList<>();
				long startTime = System.currentTimeMillis();

				try (Statement st = conn.createStatement()) {
					st.execute("DO $$ DECLARE r RECORD;" +
							"BEGIN " +
							"   FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP " +
							"       EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' CASCADE'; " +
							"   END LOOP;" +
							"END $$;");

					long logCtr, traceCtr, eventCtr;
					ResultSet resultingId;

					startTime = System.currentTimeMillis();

					for (XLog log : list) {
						resultingId = st.executeQuery("SELECT COALESCE( (SELECT MAX(id) FROM log)+1 , 0 );");
						resultingId.next();
						logCtr = resultingId.getLong(1);

						String logName = XConceptExtension.instance().extractName(log);
						insertLog(st, logCtr, logName);

						System.out.println("Try no " + (i+1) + "\tLog " + (logCtr+1) + "\tConverting extensions");
						insertExtensions(st, log.getExtensions());

						System.out.println("Try no " + (i+1) + "\tLog " + (logCtr+1) + "\tConverting classifiers");
						insertLogClassifiers(st, logCtr, log.getClassifiers());

						for (XTrace trace : log) {
							resultingId = st.executeQuery("SELECT COALESCE( (SELECT MAX(id) FROM trace)+1 , 0 );");
							resultingId.next();
							traceCtr = resultingId.getLong(1);

							System.out.println("Try no " + (i+1) + "\tLog " + (logCtr+1) + "\tConverting trace " + (traceCtr+1) + " of " + log.size());

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

						/* Removing log attributes insertion for measuring performance
						System.out.println("Try no " + (i+1) + "\tLog " + (logCtr+1) + "\tConverting log attributes");
						insertLogAttributes(st, logCtr, log.getAttributes().values().stream().filter(att -> !att.getKey().equals(XConceptExtension.KEY_NAME)).collect(Collectors.toList()), null, Scope.NONE);
						insertLogAttributes(st, logCtr, log.getGlobalTraceAttributes(), null, Scope.TRACE);
						insertLogAttributes(st, logCtr, log.getGlobalEventAttributes(), null, Scope.EVENT);
						*/
					}

					// Emptying transaction log
//					st.execute("DBCC SHRINKFILE (" + dbName + "_log, 1);");
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
					"INSERT INTO extension (id, name, prefix, uri) " +
							"VALUES (COALESCE((SELECT MAX(id) FROM extension) + 1, 0), " + name + ", " + prefix + ", " + uri + ") " +
							"ON CONFLICT DO NOTHING;"
			);
		}
	}

	private static void insertLogClassifiers(Statement stmt, long logId, List<XEventClassifier> classifierList) throws SQLException {
		for (XEventClassifier classif : classifierList) {
			String name = Commons.prepareValueForInsertion(classif.name(), 50);
			String keys = Commons.prepareValueForInsertion(String.join(", ", classif.getDefiningAttributeKeys()), 250);

			stmt.execute(
					"INSERT INTO classifier (id, name, keys, log_id) " +
							"VALUES (COALESCE((SELECT MAX(id) FROM classifier) + 1, 0), " + name + ", " + keys + ", " + logId + ") " +
							"ON CONFLICT DO NOTHING;"
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
				"IF NOT EXISTS (SELECT 1 FROM log_has_attribute "
								+ "WHERE log_id = " + logId
								+ " AND trace_global = " + traceGlobal
								+ " AND event_global = " + eventGlobal
								+ " AND attr_id = " + attributeId
							+ ") "
					+ "INSERT INTO log_has_attribute ( log_id, trace_global, event_global, attr_id, value )"
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
			"INSERT INTO event ( id, trace_id, name, transition, event_coll_id, time ) "
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
		else    // Treating all other types as literals
			type = "'literal'";

		String extIdStr;
		if (att.getExtension() != null) {
			String extName = Commons.prepareValueForInsertion(att.getExtension().getName(), 50);
			String extPrefix = Commons.prepareValueForInsertion(att.getExtension().getPrefix(), 50);
			String extUri = Commons.prepareValueForInsertion(att.getExtension().getUri().toString(), 250);

			ResultSet extId = stmt.executeQuery(
					"SELECT id FROM extension " +
							"WHERE name ILIKE " + extName +
							" AND prefix ILIKE " + extPrefix +
							" AND uri ILIKE " + extUri + ";"
			);

			extIdStr = extId.next() ? extId.getString(1) : "NULL";
		} else {
			extIdStr = "NULL";
		}

		String parentIdStr = parentId == null ? "NULL" : String.valueOf(parentId);

		ResultSet attributeIdQuery = stmt.executeQuery(
				"WITH new_attribute AS (" +
						"    INSERT INTO attribute (id, type, key, ext_id, parent_id) " +
						"    SELECT COALESCE((SELECT MAX(id) + 1 FROM attribute), 0), " + type + ", " + key + ", " + extIdStr + ", " + parentIdStr + " " +
						"    WHERE NOT EXISTS (" +
						"        SELECT 1 FROM attribute " +
						"        WHERE type = " + type +
						"          AND key = " + key +
						"          AND ext_id IS DISTINCT FROM " + extIdStr +
						"          AND parent_id IS DISTINCT FROM " + parentIdStr +
						"    ) " +
						"    RETURNING id" +
						") " +
						"SELECT id FROM new_attribute " + // Changed from 'attribute' to 'new_attribute'
						"LIMIT 1;"
		);

		// Check if the ResultSet contains a row
		if (attributeIdQuery.next()) {
			return attributeIdQuery.getLong(1); // Retrieve the id
		} else {
			throw new SQLException("No attribute ID found or inserted.");
		}
	}

}

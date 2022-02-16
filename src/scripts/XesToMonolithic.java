package scripts;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.deckfour.xes.extension.std.XConceptExtension;
import org.deckfour.xes.extension.std.XTimeExtension;
import org.deckfour.xes.model.XAttribute;
import org.deckfour.xes.model.XAttributeBoolean;
import org.deckfour.xes.model.XAttributeContinuous;
import org.deckfour.xes.model.XAttributeDiscrete;
import org.deckfour.xes.model.XAttributeLiteral;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;
import org.deckfour.xes.model.impl.XAttributeLiteralImpl;

public class XesToMonolithic {
	
	private static final String DB_URL = "jdbc:mysql://localhost:3306/Monolithic_DB";
	private static final String USER = "root";
	private static final String PWD = "password";

	public static void main(String[] args) {
		File logFile = Commons.selectLogFile();
		if (logFile == null) return;
		
		long startTime = System.currentTimeMillis();
		
		List<XLog> list = Commons.convertToXlog(logFile);
		
		try (Connection conn = Commons.getConnection(USER, PWD, DB_URL)) {
			try (Statement st = conn.createStatement()) {
				
				String tableName = Commons.getNameForDB(logFile.getName());
				
				st.execute("DROP TABLE IF EXISTS " + tableName + ";");
				
				st.execute(	// Simplified table of a real log
					"CREATE TABLE " + tableName + "("
					+ "log_id 		bigint			NOT NULL,"
					+ "log_name 	varchar(250)	NOT NULL,"
					+ "trace_id 	bigint			NOT NULL,"
					+ "trace_name 	varchar(250)	NOT NULL,"
					+ "event_id 	bigint			NOT NULL,"
					+ "event_name 	varchar(250)	NOT NULL,"
					+ "event_timestamp datetime(3)	NOT NULL,"
					+ "PRIMARY KEY( log_id, trace_id, event_id )"
					+ ");"
				);
				
				Map<String, String> headersAndValues = new LinkedHashMap<>();	// Preserving insertion order
						
				long logID = 0;
				for (XLog log : list) {
					String logName = XConceptExtension.instance().extractName(log);
					if (logName == null)	logName = "Log no. " + (logID+1) + "from: " + logFile.getName();
					
					headersAndValues.put("log_id", "'" + logID + "'");
					headersAndValues.put("log_name", "'" + logName + "'");
					
					int logSize = log.size();
					long traceID = 0;
					for (XTrace trace : log) {
						System.out.println("Log " + (logID+1) + " - Converting trace " + (traceID+1) + " of " + logSize);
						
						String traceName = XConceptExtension.instance().extractName(trace);
						if (traceName == null)	traceName = "Trace no. " + (traceID+1) + "from: " + logName;
						
						headersAndValues.put("trace_id", "'" + traceID + "'");
						headersAndValues.put("trace_name", "'" + traceName + "'");
						
						long eventID = 0;
						for (XEvent event : trace) {
							String eventName = XConceptExtension.instance().extractName(event);
							if (eventName == null)	eventName = "Event no. " + (eventID+1) + "from: " + traceName;
							
							SimpleDateFormat sdf = new SimpleDateFormat();
							sdf.applyPattern("YYYY-MM-dd HH:mm:ss.SSS");
							Date eventTimestamp = XTimeExtension.instance().extractTimestamp(event);
							String timestampStr = sdf.format(eventTimestamp);
							
							headersAndValues.put("event_id", "'" + eventID + "'");
							headersAndValues.put("event_name", "'" + eventName + "'");
							headersAndValues.put("event_timestamp", "'" + timestampStr + "'");
							
							List<String> attributeHeaders = new ArrayList<>();
							
							for (XAttribute att : event.getAttributes().values()) {
								if (!att.getKey().equals(XConceptExtension.KEY_NAME)
										&& !att.getKey().equals(XTimeExtension.KEY_TIMESTAMP)) {
									
									
									String dataType, value;
									if (att instanceof XAttributeBoolean) {
										XAttributeBoolean boolAtt = (XAttributeBoolean) att;
										dataType = "boolean";
										value = String.valueOf(boolAtt.getValue());
										
									} else if (att instanceof XAttributeContinuous) {
										XAttributeContinuous doubleAtt = (XAttributeContinuous) att;
										dataType = "double";
										value = "'" + doubleAtt.getValue() + "'";
										
									} else if (att instanceof XAttributeDiscrete) {
										XAttributeDiscrete longAtt = (XAttributeDiscrete) att;
										dataType = "bigint";
										value = "'" + longAtt.getValue() + "'";
										
									} else {	// Treating all other types as literals
										XAttributeLiteral stringAtt = new XAttributeLiteralImpl(att.getKey(), att.toString(), att.getExtension());
										dataType = "varchar(250)";
										value = "'" + stringAtt.getValue() + "'";
									}
									
									String attributeHeader = Commons.getNameForDB(att.getKey());
									// Checking if the column related to the attribute exists, if not then it will be added
									ResultSet rs = st.executeQuery("SHOW COLUMNS FROM " + tableName + " LIKE '" + attributeHeader + "';");
									if (!rs.next())
										st.execute(
											"ALTER TABLE " + tableName 
											+ " ADD " + attributeHeader + " " + dataType + ";"
										);
									
									headersAndValues.put(attributeHeader, value);
									attributeHeaders.add(attributeHeader);
								}
							}
							
									
							st.execute(
								"INSERT INTO " + tableName + " ( " + String.join(", ", headersAndValues.keySet()) + " ) "
								+ "VALUES ( " + String.join(", ", headersAndValues.values()) + " );"
							);
							
							for (String attH : attributeHeaders)
								headersAndValues.replace(attH, null);
							
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
}

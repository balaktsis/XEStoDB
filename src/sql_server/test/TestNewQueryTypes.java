package sql_server.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.List;

import commons.Commons;

public class TestNewQueryTypes {

	private static final String USER = "sa";
	private static final String PWD = "Riva96_shared_db";
	private static final String DRIVER_CLASS = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private static final String DB = "mono_sepsis";
	private static final int QUERY_TIMEOUT = 30*60; // 30 minutes
	private static final int NUM_EXECUTIONS = 5;
	
	private static List<String> queryModeList 
					= List.of("IS", "LRC", "MT", "VAL");
	private static List<String> templateList 
					= List.of("Response", "Alternate_Response", "Chain_Response",
							"Precedence", "Alternate_Precedence", "Chain_Precedence",
							"Responded_Existence"); 
	
	public static void main(String[] args) throws IOException {
		String dbName = DB;
    	String dbUrl = "jdbc:sqlserver://localhost:1433;databaseName=" + dbName + ";encrypt=true;trustServerCertificate=true;";
    	
    	try (Connection conn = Commons.getConnection(USER, PWD, dbUrl, DRIVER_CLASS)) {
    		System.out.println("Got connection to DB: " + dbName);
    		
    		try (Statement st = conn.createStatement()) {
    			st.setQueryTimeout(QUERY_TIMEOUT);
    			File createEventTableFile = Paths.get("Queries", "Event tables", "rxes+.sql").toFile();
    			StringBuilder createEventTableScript = new StringBuilder();
    			
    			if (createEventTableFile.isFile())
    				for (String line : Files.readAllLines(createEventTableFile.toPath()))
    					createEventTableScript.append(line + "\n");
    			else
    				return;
    			    			
    			for (String template : templateList) {
    				for (String queryMode : queryModeList) {
    					String queryFileName = queryMode + "_" + template;
    					System.out.println("Executing: " + queryFileName);
    					
		    			File queryFile = Paths.get("Queries", "Join", queryFileName+".sql").toFile();
		    			StringBuilder queryScript = new StringBuilder();
		    			
		    			if (queryFile.isFile())
		    				for (String line : Files.readAllLines(queryFile.toPath()))
		    					queryScript.append(line + "\n");
		    			else
		    				return;
		    			
		    			
		    			for (int i=0; i<NUM_EXECUTIONS; i++) {
		    				try {
				    			ResultSet execTime = st.executeQuery(
				    				"DECLARE @t1 DATETIME = GETDATE();\n"
				    				
				    				+ createEventTableScript.toString()
				    				+ queryScript.toString()
				    				
				    				+ "DECLARE @t2 DATETIME = GETDATE();\n"
				    				+ "SELECT DATEDIFF(millisecond, @t1, @t2) AS elapsed_time;"
				    			);
				    			
				    			execTime.next();
				    			System.out.println(execTime.getLong(1));
		    				} catch (SQLTimeoutException e) {
		    					System.out.println("Query took longer than " + QUERY_TIMEOUT/60 + " minutes to execute");
		    					break;
		    				}
		    			}
		    			
		    			System.out.println("");
    				}
    			}
    		}
    		
    		System.out.println("Complete!");
    		
    	} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}

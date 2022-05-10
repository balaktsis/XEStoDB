package commons;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.swing.JFileChooser;
import javax.swing.filechooser.FileNameExtensionFilter;

import org.deckfour.xes.extension.std.XLifecycleExtension;
import org.deckfour.xes.extension.std.XLifecycleExtension.StandardModel;
import org.deckfour.xes.in.XMxmlGZIPParser;
import org.deckfour.xes.in.XMxmlParser;
import org.deckfour.xes.in.XParser;
import org.deckfour.xes.in.XesXmlGZIPParser;
import org.deckfour.xes.in.XesXmlParser;
import org.deckfour.xes.model.XEvent;
import org.deckfour.xes.model.XLog;
import org.deckfour.xes.model.XTrace;

public class Commons {	
	public static Connection getConnection(String user, String pwd, String url, String driverClass) throws SQLException, ClassNotFoundException {

		Class.forName(driverClass);
		
	    Connection conn = null;
	    Properties connectionProps = new Properties();
	    connectionProps.put("user", user);
	    connectionProps.put("password", pwd);

        conn = DriverManager.getConnection(url, connectionProps);
	    
	    System.out.println("User \"" + user + "\" connected to database \"" + url + "\".");
	    return conn;
	}
        
    public static File selectLogFile() {
    	
    	JFileChooser chooser = new JFileChooser(System.getProperty("user.dir"));
        FileNameExtensionFilter filter = new FileNameExtensionFilter("XES Log file", "xes", ".mxml", ".xes.gz", ".mxml.gz");
        chooser.setFileFilter(filter);
        
        int returnVal = chooser.showOpenDialog(null);
        if (returnVal == JFileChooser.APPROVE_OPTION) {
            System.out.println("You chose to open this file: " + chooser.getSelectedFile().getName());
            return chooser.getSelectedFile();
        } else {
        	System.err.println("No file selected.");
			return null;
        }
    }
    
    public static List<XLog> convertToXlog(File logFile) {
    	XParser parser = null;
		
    	if (logFile.getName().toLowerCase().endsWith(".mxml"))
			parser = new XMxmlParser();
    	else if (logFile.getName().toLowerCase().endsWith(".mxml.gz"))
			parser = new XMxmlGZIPParser();	
		else if (logFile.getName().toLowerCase().endsWith(".xes"))
			parser = new XesXmlParser();
		else if (logFile.getName().toLowerCase().endsWith(".xes.gz"))
			parser = new XesXmlGZIPParser();
			
		
		List<XLog> logList = new ArrayList<>();
    	
    	try {
    		
    		if (parser!=null && parser.canParse(logFile))
    			logList = parser.parse(logFile);
    		
    		// Assigning a COMPLETE transition if not specified in the log file
    		for (XLog log : logList)
    			for (XTrace trace : log)
    				for (XEvent evt : trace)
    					if (XLifecycleExtension.instance().extractTransition(evt) == null)
    						XLifecycleExtension.instance().assignStandardTransition(evt, StandardModel.COMPLETE);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return logList;
	}
    
    public static String getNameForDB(String name) {
    	return name.replaceAll("(\\s|-|\\.|:|\\?)+", "_");
    }
    
    public static String prepareValueForInsertion(Object value, int maxLen) {
    	return value != null ? "'" + truncateIfNecessary(value, maxLen) + "'" : "NULL";
    }
    
    public static String truncateIfNecessary(Object obj, int maxLen) {
    	String objStr = obj.toString();
    	return objStr.substring(0, Math.min(objStr.length(), maxLen));
    }
    
    public static String selectPredicate(String val) {
    	return val.equals("NULL") ? " is " : " = ";
    }
}

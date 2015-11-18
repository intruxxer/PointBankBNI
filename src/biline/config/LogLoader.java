package biline.config;

/**
 * LogLoader
 *
 * Class untuk menyimpan log aplikasi
 *
 * @package		biline.config
 * @author		Developer Team
 * @copyright   Copyright (c) 2015, PT. Biline Aplikasi Digital for PT Bank Negara Indonesia Tbk,,
 */
/*
 * ------------------------------------------------------
 *  Memuat package dan library
 * ------------------------------------------------------
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.log4j.PropertyConfigurator;

public class LogLoader {

	public LogLoader() {
		//Constructor
	}
	
	public static Logger LOG;
	
	/*
	public static PropertiesLoader getInstance() {
		return new PropertiesLoader();
	}
	*/
	
	public static Logger getLog() {
		if (LOG == null) {
		    PropertyConfigurator.configure(LogLoader.class.getClassLoader().getResourceAsStream("log4j.properties"));
			LOG = LoggerFactory.getLogger(LogLoader.class);
		
			try {		 
				/*input = PropertiesLoader.class.getClassLoader().getResourceAsStream("config.properties");
				//input = new FileInputStream("config.properties");
				
				// load a properties file
				LOG.load(input);*/
		
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return LOG;
	}
	
	public static void setInfo(String clsName, String msg) {
        //String temp = new Timestamp(System.currentTimeMillis()).toString() + "::" + clsName;
		getLog().info("{}: {}", clsName, msg);
	}
	
	public static void setError(String clsName, Object msg) {
        //String temp = new Timestamp(System.currentTimeMillis()).toString() + "::" + clsName;
		getLog().error("{}: {}", clsName, msg);
	}

}

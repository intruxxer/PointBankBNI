package biline.config;

/**
 * PropertiesLoader
 *
 * Class untuk membaca file config.properties 
 *
 * @package		biline.config
 * @author		Developer Team
 * @copyright   Copyright (c) 2015, PT. Biline Aplikasi Digital for PT Bank Negara Indonesia Tbk,
 */
/*
 * ------------------------------------------------------
 *  Memuat package dan library
 * ------------------------------------------------------
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoader {

	public PropertiesLoader() {
		//Constructor
	}
	
	private static Properties PROP;
	
	protected static Properties getProp() {
		if (PROP == null) {
			PROP = new Properties();
			InputStream input = null;		 
			try {		 
				input = PropertiesLoader.class.getClassLoader().getResourceAsStream("config.properties");
				//input = new FileInputStream("config.properties");
				//input = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties");
			    if (input == null)
			    {
			      throw new FileNotFoundException("Property file not found in the classpath");
			    }
				// load a properties file
				PROP.load(input);
				
			} catch (IOException ex) {
				ex.printStackTrace();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return PROP;
	}
	
	public static String getProperty(String propStr) {
		return getProp().getProperty(propStr);		
	}

}

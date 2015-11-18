package biline.db;

/**
 * MysqlConnect
 *
 * Class untuk koneksi ke database. Menggunakan Mysql
 *
 * @package		biline.config
 * @author		Developer Team
 * @copyright   Copyright (c) 2015, PT. Biline Aplikasi Digital for PT Bank Negara Indonesia Tbk,
 */
/* ------------------------------------------------------
 *  Memuat package dan library
 * ------------------------------------------------------
 */

import biline.config.*;
import biline.config.PropertiesLoader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
//import com.microsoft.sqlserver.jdbc.*;

public class MysqlConnect {
	protected static Connection connection;
	
	public MysqlConnect() throws SQLException, ClassNotFoundException {
		//Constructor
		connection = null;
        
		try {
			//There are two alternatives MySQL JDBC JAR class.
            Class.forName("com.mysql.jdbc.Driver");
			//Specify the JTDS driver being used. 
            //Class.forName("net.sourceforge.jtds.jdbc.Driver"); 
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Cannot find the driver in the classpath!", e);
        }
        
        try {
        	connection = DriverManager.getConnection(
            		"jdbc:mysql://"+ PropertiesLoader.getProperty("DB_URL") 
            		+ ":"+ PropertiesLoader.getProperty("DB_PORT") +"/" + PropertiesLoader.getProperty("DB_NAME") + "?" 
            		+ "user=" + PropertiesLoader.getProperty("DB_USERNAME") +"&password=" + PropertiesLoader.getProperty("DB_PASSWORD")
            		);
            if(connection != null){
            	//System.out.println("Database connected via channel: "+String.valueOf(connection));
            	LogLoader.setInfo(MysqlConnect.class.getSimpleName(), "[CONSTRUCTOR] Connection to MySQL DB established.");
            }
        } catch (SQLException e) {
            throw new RuntimeException("[CONSTRUCTOR] Cannot connect to the database!", e);
        }
	}
	
	public Connection getConnection(){
		//System.out.println("Database Connection to be retrieved: " + String.valueOf(connection));
    	//LogLoader.setInfo(MysqlConnect.class.getSimpleName(), "[GET CONN.] Connection to MySQL DB retrieved.");
		return connection;
	}
	
	public void openConnection(){
		try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(
            		"jdbc:mysql://"+ PropertiesLoader.getProperty("DB_URL") 
            		+ ":"+ PropertiesLoader.getProperty("DB_PORT") +"/" + PropertiesLoader.getProperty("DB_NAME") + "?" 
            		+ "user=" + PropertiesLoader.getProperty("DB_USERNAME") +"&password=" + PropertiesLoader.getProperty("DB_PASSWORD")
            		);
            if(connection != null){
            	//System.out.println("Database connected via channel: "+String.valueOf(connection));
            	LogLoader.setInfo(MysqlConnect.class.getSimpleName(), "[OPEN] Connection to MySQL DB established.");
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("[OPEN CONN.] Cannot find the driver in the classpath!", e);
        } catch (SQLException e) {
            throw new RuntimeException("[OPEN CONN.] Cannot connect to the database!", e);
        }
	}
	
	public void closeConnection() throws SQLException{
        getConnection().close();
        //System.out.println("Database disconnected!");
    	LogLoader.setInfo(MysqlConnect.class.getSimpleName(), "[CLOSE] Connection to MySQL DB terminated.");
    }

}

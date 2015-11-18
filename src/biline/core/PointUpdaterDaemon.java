package biline.core;

import biline.config.*;
import biline.db.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
//import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.opencsv.CSVReader;

public class PointUpdaterDaemon {

	private static Connection con;
	private static Statement stm;
	//private static ResultSet rs;
	private static int affectedRow;
	private static int headerLine;
	private static MysqlConnect db_object;
	private static String workingDir;
	private static String monthlyFile;
	private static String quarterlyFile;
	private static String grandprizeFile;
	private static char recSeparator;
	private static char escChar;
	private static Boolean statCreate;
	private static Boolean statMonthly;
	private static Boolean statQuarterly;
	private static Boolean statGrandprize;
	
	private static int recordCounter;
	
	private static PointUpdaterDaemon pointDaemon;
	
	public PointUpdaterDaemon() {
		con              = null; 
		stm              = null; 
	//	rs               = null; 
	//	affectedRow      = 0;
		recordCounter	 = 0;
		statCreate       = false;
		statMonthly      = false;
		statQuarterly    = false;
		statGrandprize   = false;
		workingDir       = System.getProperty("user.dir");
		monthlyFile		 = PropertiesLoader.getProperty("POINT_MONTHLY_FILE");
		quarterlyFile	 = PropertiesLoader.getProperty("POINT_QUARTERLY_FILE");
		grandprizeFile	 = PropertiesLoader.getProperty("POINT_GRANDPRIZE_FILE");
		recSeparator	 = '|'; 
		escChar			 = '\''; 
		headerLine		 = 1;
		
		//Establishing a Single DB Connection.
		//This Connection is usable across app life-cycle.
		try{
			db_object = new MysqlConnect();
			con 	  = db_object.getConnection(); 
		} 
		catch(ClassNotFoundException nfe){ nfe.printStackTrace(); } 
		catch(SQLException sqle){ sqle.printStackTrace(); }
		
	}
	
	public static String getCurrentTimeStamp() {
	    return new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS").format(new Date());
	}

	@SuppressWarnings("unused")
	public static void main(String[] args) throws IOException, SQLException {
		//Evaluation Timer
		System.out.println("[MAIN PROCESS] Started: " + getCurrentTimeStamp());
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), "[MAIN PROCESS] Started: " + getCurrentTimeStamp());
		long startTime = System.currentTimeMillis();
		
		pointDaemon = new PointUpdaterDaemon();
		
		//Choose these options: 
		
		//(A) Fresh start with ALL INSERT. Separated Tables.
		//3 Separated Point Tables would be Created, then Inserted.
		
		//Creating Table must be DONE BEFORE running INSERTION Threads.
		pointDaemon.createTruncateSeparatedPointTables();
		
		//1. With Single, Blocking Process (No Thread Programming)
			if(false)
			{
				pointDaemon.insertMonthlyPoint();
				pointDaemon.insertQuarterlyPoint();
				pointDaemon.insertGrandprizePoint();
			}
		//2. With Concurrent, Multi-Thread Programming
			else if(true)
			{
				Runnable monthlyRunnable = new Runnable(){
				     public void run(){
				        System.out.println("[THREAD] [" + getCurrentTimeStamp() + "]" + "Thread 1: Inserting Monthly Point Records..");
				        LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), "[THREAD 1 START] [" + getCurrentTimeStamp() + "]");
				        try {
							pointDaemon.insertMonthlyPoint();
						} catch (IOException | SQLException e) {
							e.printStackTrace();
						}
				        System.out.println("[THREAD] [" + getCurrentTimeStamp() + "]" + "Thread 1: Finished.");
				        LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), "[THREAD 2 STOP] [" + getCurrentTimeStamp() + "]");
				     }
				};
				Runnable quarterlyRunnable = new Runnable(){
				     public void run(){
				        System.out.println("[THREAD] [" + getCurrentTimeStamp() + "]" + "Thread 2: Inserting Quarterly Point Records..");
				        LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), "[THREAD 2 START] [" + getCurrentTimeStamp() + "]");
				        try {
				        	pointDaemon.insertQuarterlyPoint();
						} catch (IOException | SQLException e) {
							e.printStackTrace();
						}
				        System.out.println("[THREAD] [" + getCurrentTimeStamp() + "]" + "Thread 2: Finished.");
				        LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), "[THREAD 2 STOP] [" + getCurrentTimeStamp() + "]");
				     }
				};
				Runnable grandprizeRunnable = new Runnable(){
				     public void run(){
				        System.out.println("[" + getCurrentTimeStamp() + "]" + "Thread 3: Inserting Grand Prize Point Records..");
				        LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), "[THREAD 3 START] [" + getCurrentTimeStamp() + "]");
				        try {	
				    		pointDaemon.insertGrandprizePoint();
						} catch (IOException | SQLException e) {
							e.printStackTrace();
						}
				        System.out.println("[" + getCurrentTimeStamp() + "]" + "Thread 3: Finished.");
				        LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), "[THREAD 3 STOP] [" + getCurrentTimeStamp() + "]");
				     }
				};
		
				Thread monthlyThread    = new Thread(monthlyRunnable);
				Thread quarterlyThread  = new Thread(quarterlyRunnable);
				Thread grandprizeThread = new Thread(grandprizeRunnable);
				
				//Setting Daemon Mode if Necessary
				
				//monthlyThread.setDaemon(true);
				//quarterlyThread.setDaemon(true);
				//grandprizeThread.setDaemon(true);
				
				monthlyThread.start();
				quarterlyThread.start();
				grandprizeThread.start();
				
				//Processing Time, If using No Thread Concept
				long endTime = System.currentTimeMillis();
				long duration = endTime - startTime;
				System.out.println(
						"[MAIN PROCESS] Duration: " + duration + " ms.");
				LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
						"[MAIN PROCESS] Finished: " + getCurrentTimeStamp() + "(" + duration + " ms).");
			}
		

		//(B) Fresh start with ONE INSERT, TWO UPDATES. One united point Table.
		//1 Point Table would be Created, then Inserted once, Updated twice.
			/*
			pointDaemon.createTruncateUnitedPointTable(); 
			pointDaemon.insertMonthlyPoint();
			pointDaemon.updateQuarterlyPoint();     
			pointDaemon.updateGrandprizePoint();
			*/
		//(C) Current start with existing records
			/*
			pointDaemon.updateMonthlyPoint(); 
			pointDaemon.updateQuarterlyPoint();     
			pointDaemon.updateGrandprizePoint();
			*/
			
		//Final Result Report
		if(statCreate)
			System.out.println("[RESULT]: TABLES CREATED.");
		if(statMonthly)
			System.out.println("[RESULT]: MONTHLY POINTS INSERTED.");
		if(statQuarterly)
			System.out.println("[RESULT]: QUARTERLY POINTS INSERTED.");
		if(statGrandprize)
			System.out.println("[RESULT]: GRAND PRIZE POINTS INSERTED.");
	}
	
	public void createTruncateUnitedPointTable()
	{
			System.out.println("Starting creating a table if not exists yet or truncating non-empty table..");
			System.out.println("[TABLE] Started: " + getCurrentTimeStamp());
			LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(),
					"[TABLE] Started: " + getCurrentTimeStamp());
			//CREATE TABLE & TRUNCATE AL PREV. DATA
			String createPointTableQuery = 
						"CREATE TABLE IF NOT EXISTS `tbl_points` (" +
						" `point_id` int(11) NOT NULL NOT NULL DEFAULT '0'," +
						" `point_no` int(7) NOT NULL DEFAULT '0'," +
						" `point_accnum` varchar(20) NOT NULL DEFAULT '0'," +
						" `point_cardno` varchar(16) NOT NULL DEFAULT '0'," +
						" `point_monthly` int(6) NOT NULL DEFAULT '0'," +
						" `point_quarterly` int(6) NOT NULL DEFAULT '0'," +
						" `point_grandprize` int(6) NOT NULL DEFAULT '0'," +
						" `deleted` int(1) NOT NULL DEFAULT '0'," +
						" `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP" +
						") ENGINE=InnoDB DEFAULT CHARSET=latin1;"
						+ "ALTER TABLE `tbl_points` ADD PRIMARY KEY (`point_id`);"
						+ "ALTER TABLE `tbl_points` MODIFY `point_id` int(11) NOT NULL AUTO_INCREMENT;";
				
			String truncatePointTableQuery = "TRUNCATE TABLE  `tbl_points`;";
				
			try {
		     	if(con == null){
		     		db_object.openConnection();
		  			con = db_object.getConnection();
		  	    }
			 	stm 		 = con.createStatement();
			 	affectedRow  = stm.executeUpdate(createPointTableQuery);
			 	affectedRow  = stm.executeUpdate(truncatePointTableQuery);
			} 
		    catch (SQLException e) { e.printStackTrace(); } 
		    finally
		    {
		 		if(con != null){
					try { db_object.closeConnection(); } 
					catch (SQLException e) { e.printStackTrace(); } 
					finally { con = null; }
		 		}
		 		statCreate = true;
			}
			System.out.println("[TABLE] Finished: " + getCurrentTimeStamp());
			LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(),
					"[TABLE] Finished: " + getCurrentTimeStamp());
	}
	public void createTruncateSeparatedPointTables()
	{
			System.out.println("Starting creating three separated point tables (if not exists yet) or truncating non-empty tables..");
			System.out.println("[TABLE] Started: " + getCurrentTimeStamp());
			LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(),
					"[TABLE] Started: " + getCurrentTimeStamp());
			//CREATE TABLES & TRUNCATE AL PREV. DATA
			String createMonthlyPointTableQuery = 
						"CREATE TABLE IF NOT EXISTS `tbl_points_monthly` (" +
						" `point_id` int(11) NOT NULL auto_increment," +
						" `point_no` int(7) NOT NULL DEFAULT '0'," +
						" `point_accnum` varchar(20) NOT NULL DEFAULT '0'," +
						" `point_cardno` varchar(16) NOT NULL DEFAULT '0'," +
						" `point_monthly` int(6) NOT NULL DEFAULT '0'," +
						" `deleted` int(1) NOT NULL DEFAULT '0'," +
						" `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP" +
						") ENGINE=InnoDB DEFAULT CHARSET=latin1;";
				
			String addPKMonthlyPointTableQuery	  = "ALTER TABLE `tbl_points_monthly` ADD PRIMARY KEY (`point_id`);";
			String addAIMonthlyPointTableQuery    = "ALTER TABLE `tbl_points_monthly` MODIFY `point_id` int(11) NOT NULL AUTO_INCREMENT;";
			String truncateMonthlyPointTableQuery = "TRUNCATE TABLE  `tbl_points_monthly`;";
			
			String createQuarterlyPointTableQuery = 
					"CREATE TABLE IF NOT EXISTS `tbl_points_quarterly` (" +
					" `point_id` int(11) NOT NULL," +
					" `point_no` int(7) NOT NULL DEFAULT '0'," +
					" `point_accnum` varchar(20) NOT NULL DEFAULT '0'," +
					" `point_cardno` varchar(16) NOT NULL DEFAULT '0'," +
					" `point_quarterly` int(6) NOT NULL DEFAULT '0'," +
					" `deleted` int(1) NOT NULL DEFAULT '0'," +
					" `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP" +
					") ENGINE=InnoDB DEFAULT CHARSET=latin1;";
			
			String addPKQuarterlyPointTableQuery    = "ALTER TABLE `tbl_points_quarterly` ADD PRIMARY KEY (`point_id`);";
			String addAIQuarterlyPointTableQuery    = "ALTER TABLE `tbl_points_quarterly` MODIFY `point_id` int(11) NOT NULL AUTO_INCREMENT;";
			String truncateQuarterlyPointTableQuery = "TRUNCATE TABLE  `tbl_points_quarterly`;";
		
			String createGrandprizePointTableQuery = 
				"CREATE TABLE IF NOT EXISTS `tbl_points_grandprize` (" +
				" `point_id` int(11) NOT NULL," +
				" `point_no` int(7) NOT NULL DEFAULT '0'," +
				" `point_accnum` varchar(20) NOT NULL DEFAULT '0'," +
				" `point_cardno` varchar(16) NOT NULL DEFAULT '0'," +
				" `point_grandprize` int(6) NOT NULL DEFAULT '0'," +
				" `deleted` int(1) NOT NULL DEFAULT '0'," +
				" `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP" +
				") ENGINE=InnoDB DEFAULT CHARSET=latin1;";
		
			String addPKGrandprizePointTableQuery    = "ALTER TABLE `tbl_points_grandprize` ADD PRIMARY KEY (`point_id`);";
			String addAIGrandprizePointTableQuery    = "ALTER TABLE `tbl_points_grandprize` MODIFY `point_id` int(11) NOT NULL AUTO_INCREMENT;";
			String truncateGrandprizePointTableQuery = "TRUNCATE TABLE  `tbl_points_grandprize`;";
				
			try {
		     	if(con == null){
		     		db_object.openConnection();
		  			con = db_object.getConnection();
		  	    }
			 	stm 		 = con.createStatement();
			 	affectedRow  = stm.executeUpdate(createMonthlyPointTableQuery);
			 	affectedRow  = stm.executeUpdate(addPKMonthlyPointTableQuery);
			 	affectedRow  = stm.executeUpdate(addAIMonthlyPointTableQuery);
			 	affectedRow  = stm.executeUpdate(truncateMonthlyPointTableQuery);
			 	
			 	affectedRow  = stm.executeUpdate(createQuarterlyPointTableQuery);
			 	affectedRow  = stm.executeUpdate(addPKQuarterlyPointTableQuery);
			 	affectedRow  = stm.executeUpdate(addAIQuarterlyPointTableQuery);
			 	affectedRow  = stm.executeUpdate(truncateQuarterlyPointTableQuery);
			 	
			 	affectedRow  = stm.executeUpdate(createGrandprizePointTableQuery);
			 	affectedRow  = stm.executeUpdate(addPKGrandprizePointTableQuery);
			 	affectedRow  = stm.executeUpdate(addAIGrandprizePointTableQuery);
			 	affectedRow  = stm.executeUpdate(truncateGrandprizePointTableQuery);
			 	
			} 
		    catch (SQLException e) { e.printStackTrace(); } 
		    finally
		    {
		 		if(con != null){
					try { db_object.closeConnection(); } 
					catch (SQLException e) { e.printStackTrace(); } 
					finally { con = null; }
		 		}
		 		statCreate = true;
			}
			System.out.println("Finished Creating Tables: " + getCurrentTimeStamp());
			System.out.println("[TABLE] Finished: " + getCurrentTimeStamp());
			LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(),
					"[TABLE] Finished: " + getCurrentTimeStamp());
	}
	
	public void insertMonthlyPoint() throws IOException, SQLException
	{
		recordCounter = 0;
		System.out.println("Starting inserting monthly point records into points' table..");
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
				"[INSERT MONTHLY] Started: " + getCurrentTimeStamp());
		//INSERT MONTHLY - Supposed to be executed on INIT w/CREATE TABLE function
		//Binding a single input stream CSV reader & its buffer 
		CSVReader monthlyReader = new CSVReader(new FileReader(workingDir + monthlyFile), recSeparator, escChar, headerLine);
		String [] monthly;
		String insertMonthlyQuery;
		
		if(con == null){
 			db_object.openConnection();
			con = db_object.getConnection();
		}
		
		try {
				stm 			= con.createStatement();
				while ((monthly = monthlyReader.readNext()) != null) 
				{
					  //System.out.println(monthly[0] + monthly[1] + monthly[2]);
					  insertMonthlyQuery = "INSERT INTO tbl_points_monthly (point_cardno, point_monthly) VALUES ('" + monthly[1].trim() + "', " + monthly[2].trim() + ");";
					  stm.addBatch(insertMonthlyQuery);
				      //stm 	     = con.createStatement();
				      //affectedRow  = stm.executeUpdate(insertMonthlyQuery);
					  recordCounter++;
				}
				stm.executeBatch();
		}
		catch (FileNotFoundException e) { e.printStackTrace(); } 
		catch (SQLException e) { e.printStackTrace(); } 
		finally
		{
			stm.close();
	     	if(con != null){
	     		try { db_object.closeConnection(); } 
	     		catch (SQLException e) { e.printStackTrace(); } 
	     		finally { con = null; }
	     	}
		}
		
		statMonthly = true;
		System.out.println("Monthly Records Inserted:" + recordCounter);
		System.out.println("Monthly Records Finished Time: " + getCurrentTimeStamp());
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
				"[INSERT MONTHLY] Finished: " + getCurrentTimeStamp());
	}
	
	public void updateMonthlyPoint() throws IOException, SQLException
	{
		recordCounter = 0;
		System.out.println("Starting updating monthly point records into points' table..");
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
				"[UPDATE MONTHLY] Started: " + getCurrentTimeStamp());
		//UPDATE MONTHLY
		//Binding a single input stream CSV reader & its buffer 
		CSVReader monthlyReader = new CSVReader(new FileReader(workingDir + monthlyFile), recSeparator, escChar, headerLine);
		String [] monthly;
		String updateMonthlyQuery;
		
		if(con == null){
 			db_object.openConnection();
			con = db_object.getConnection();
	    }
		
		try 
		{
	     	  	stm 	        = con.createStatement();
				while ((monthly = monthlyReader.readNext()) != null) 
				{
					  //System.out.println(monthly[0] + monthly[1] + monthly[2]);
					  updateMonthlyQuery = "UPDATE tbl_points SET point_monthly = " + monthly[2].trim() + " WHERE  point_cardno = '" + monthly[1].trim() + "';";
					  stm.addBatch(updateMonthlyQuery);
					  recordCounter++;
				}
				stm.executeBatch();
		} 
		catch (FileNotFoundException e) { e.printStackTrace(); } 
		catch (SQLException e) { e.printStackTrace(); } 
		finally 
		{  
			stm.close();
	     	if(con != null){
	     		try { db_object.closeConnection(); } 
	     		catch (SQLException e) { e.printStackTrace(); } 
	     		finally { con = null; }
	     	}
     	}
		
		statMonthly = true;
		System.out.println("Updated Monthly Records:" + recordCounter);
		System.out.println("Monthly Records Update Finished Time: " + getCurrentTimeStamp());
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
				"[UPDATE MONTHLY] Finished: " + getCurrentTimeStamp());
	}
	
	public void insertQuarterlyPoint() throws IOException, SQLException
	{
		recordCounter = 0;
		System.out.println("Starting inserting Quarterly point records into points' table..");
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
				"[INSERT QUARTERLY] Started: " + getCurrentTimeStamp());
		//INSERT MONTHLY - Supposed to be executed on INIT w/CREATE TABLE function
		//Binding a single input stream CSV reader & its buffer 
		CSVReader quarterlyReader = new CSVReader(new FileReader(workingDir + quarterlyFile), recSeparator, escChar, headerLine);
		String [] quarterly;
		String insertQuarterlyQuery;
		
		if(con == null){
 			db_object.openConnection();
			con = db_object.getConnection();
		}
		
		try {
				stm 			  = con.createStatement();
				while ((quarterly = quarterlyReader.readNext()) != null) 
				{
					  //System.out.println(quarterly[0] + quarterly[1] + quarterly[2]);
					  insertQuarterlyQuery = "INSERT INTO tbl_points_quarterly (point_cardno, point_quarterly) VALUES ('" + quarterly[1].trim() + "', " + quarterly[2].trim() + ");";
					  stm.addBatch(insertQuarterlyQuery);
				      //stm 	     = con.createStatement();
				      //affectedRow  = stm.executeUpdate(updateQuarterlyQuery);
					  recordCounter++;
				}
				stm.executeBatch();
		}
		catch (FileNotFoundException e) { e.printStackTrace(); } 
		catch (SQLException e) { e.printStackTrace(); } 
		finally
		{
			stm.close();
	     	if(con != null){
	     		try { db_object.closeConnection(); } 
	     		catch (SQLException e) { e.printStackTrace(); } 
	     		finally { con = null; }
	     	}
		}
		
		statQuarterly = true;
		System.out.println("Quarterly Records Inserted:" + recordCounter);
		System.out.println("Quarterly Records Finished Time: " + getCurrentTimeStamp());
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
				"[INSERT QUARTERLY] Finished: " + getCurrentTimeStamp());
	}
	
	public void updateQuarterlyPoint() throws IOException, SQLException
	{
		recordCounter = 0;
		System.out.println("Starting updating quarterly point records into points' table..");
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
				"[UPDATE QUARTERLY] Started: " + getCurrentTimeStamp());
		//UPDATE QUARTERLY
		CSVReader quarterlyReader = new CSVReader(new FileReader(workingDir + quarterlyFile), recSeparator, escChar, headerLine);
		String [] quarterly;
		String updateQuarterlyQuery;
		
		if(con == null){
			   db_object.openConnection();
			   con = db_object.getConnection();
		}
		
		try 
		{
			  stm 		        = con.createStatement();
			  while ((quarterly = quarterlyReader.readNext()) != null) 
			  {
				  //System.out.println(quarterly[0] + quarterly[1] + quarterly[2]);
				  updateQuarterlyQuery = "UPDATE tbl_points SET point_quarterly = " + quarterly[2].trim() + " WHERE point_cardno = '" + quarterly[1].trim() + "';";
				  stm.addBatch(updateQuarterlyQuery);
				  recordCounter++;
			  }
			  stm.executeBatch();
		} 
		catch (FileNotFoundException e) { e.printStackTrace(); }
		catch (SQLException e) { e.printStackTrace(); } 
		finally
		{
			   stm.close();
			   if(con != null){
			  		try { db_object.closeConnection(); } 
			  		catch (SQLException e) { e.printStackTrace(); } 
			  		finally { con = null; }
			   }
		}
		
		statQuarterly = true;
		System.out.println("Updated Quarterly Records: " + recordCounter);
		System.out.println("Quarterly Records Finished Time: " + getCurrentTimeStamp());
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
				"[UPDATE QUARTERLY] Finished: " + getCurrentTimeStamp());
	}
	
	public void insertGrandprizePoint() throws IOException, SQLException
	{
		recordCounter = 0;
		System.out.println("Starting inserting Grand Prize point records into points' table..");
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
				"[INSERT GRANDPRIZE] Started: " + getCurrentTimeStamp());
		//INSERT MONTHLY - Supposed to be executed on INIT w/CREATE TABLE function
		//Binding a single input stream CSV reader & its buffer 
		CSVReader grandprizeReader = new CSVReader(new FileReader(workingDir + grandprizeFile), recSeparator, escChar, headerLine);
		String [] grandprize;
		String insertGrandprizeQuery;
		
		if(con == null){
 			db_object.openConnection();
			con = db_object.getConnection();
		}
		
		try {
				stm 			   = con.createStatement();
				while ((grandprize = grandprizeReader.readNext()) != null) 
				{
					  //System.out.println(monthly[0] + monthly[1] + monthly[2]);
					  insertGrandprizeQuery = "INSERT INTO tbl_points_grandprize (point_cardno, point_grandprize) VALUES ('" + grandprize[1].trim() + "', " + grandprize[2].trim() + ");";
					  stm.addBatch(insertGrandprizeQuery);
				      //stm 	     = con.createStatement();
				      //affectedRow  = stm.executeUpdate(updateGrandprizeQuery);
					  recordCounter++;
				}
				stm.executeBatch();
		}
		catch (FileNotFoundException e) { e.printStackTrace(); } 
		catch (SQLException e) { e.printStackTrace(); } 
		finally
		{
			stm.close();
	     	if(con != null){
	     		try { db_object.closeConnection(); } 
	     		catch (SQLException e) { e.printStackTrace(); } 
	     		finally { con = null; }
	     	}
		}
		
		statGrandprize = true;
		System.out.println("Grand Prize Records Inserted:" + recordCounter);
		System.out.println("Grand Prize Records Finished Time: " + getCurrentTimeStamp());
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
				"[INSERT GRANDPRIZE] Finished: " + getCurrentTimeStamp());
	}
	
	public void updateGrandprizePoint() throws IOException, SQLException
	{
		recordCounter = 0;
		System.out.println("Starting updating grand prize point records into points' table..");
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
				"[UPDATE GRANDPRIZE] Started: " + getCurrentTimeStamp());
		//UPDATE GRANDPRIZE
		CSVReader grandprizeReader = new CSVReader(new FileReader(workingDir + grandprizeFile), recSeparator, escChar, headerLine); 
		String [] grandprize;
		String updateGrandprizeQuery;
		
		if(con == null){
	     	db_object.openConnection();
	  		con = db_object.getConnection();
		}
		
		try 
		{
			stm 		  	   = con.createStatement();
			while ((grandprize = grandprizeReader.readNext()) != null) 
			{
				//System.out.println(grandprize[0] + grandprize[1] + grandprize[2]);
				updateGrandprizeQuery = "UPDATE tbl_points SET point_grandprize = " + grandprize[2].trim() + " WHERE point_cardno = '" + grandprize[1].trim() + "';";
				stm.addBatch(updateGrandprizeQuery);
				recordCounter++;
			}
			 stm.executeBatch();
		} 
		catch (FileNotFoundException e) { e.printStackTrace(); }
		catch (SQLException e) { e.printStackTrace(); } 
		finally
		{
			stm.close();
		     if(con != null){
		  		try { db_object.closeConnection(); } 
		  		catch (SQLException e) { e.printStackTrace(); } 
		  		finally { con = null; }
		     }
	    }
		
		statGrandprize = true;
		System.out.println("Updated Grand Prize Records: " + recordCounter);
		System.out.println("Grand Prize Records Finished Time: " + getCurrentTimeStamp());
		LogLoader.setInfo(PointUpdaterDaemon.class.getSimpleName(), 
				"[UPDATE GRANDPRIZE] Finished: " + getCurrentTimeStamp());
	}
		
}

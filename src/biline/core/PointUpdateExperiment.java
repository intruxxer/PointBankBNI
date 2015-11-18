package biline.core;

import java.util.Calendar;
import java.util.Date;
//import java.util.GregorianCalendar;

public class PointUpdateExperiment {

	public PointUpdateExperiment() {
		
	}

	public static void main(String[] args) {
		Calendar c = Calendar.getInstance(); 
	    //c.set(Calendar.DAY_OF_MONTH, 1);
	    
	    c.add(Calendar.MONTH, -1);
	    // set DATE to 1, so first date of previous month
	    c.set(Calendar.DATE, 1);
	    
	    System.out.println("Max. Date: " + c.getActualMaximum(Calendar.DATE)); 

	    String[] strMonths = new String[] { "Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli", "Agustus", "September", "Oktober", "November", "Desember" };
	    System.out.println("Current Date is : " + c.get(Calendar.DATE));
	    System.out.println("Current Month is : " + (c.get(Calendar.MONTH) + 1) + " (" + strMonths[c.get(Calendar.MONTH)] + ")");
	    System.out.println("Current Year is : " + c.get(Calendar.YEAR));
	    
	    String s = "Periode Poin : 1 - " + c.getActualMaximum(Calendar.DATE) + " " + strMonths[c.get(Calendar.MONTH)] + " " + c.get(Calendar.YEAR);
	    System.out.println(s);
	    
	    Calendar aCalendar = Calendar.getInstance();
	    aCalendar.set(Calendar.DATE, 1);
	    aCalendar.add(Calendar.DAY_OF_MONTH, -1);
	    Date lastDateOfPreviousMonth = aCalendar.getTime();
	    aCalendar.set(Calendar.DATE, 1);
	    Date firstDateOfPreviousMonth = aCalendar.getTime();
	    System.out.println("");
	    System.out.println(firstDateOfPreviousMonth.toString() + "-" + lastDateOfPreviousMonth.toString());

	}

}

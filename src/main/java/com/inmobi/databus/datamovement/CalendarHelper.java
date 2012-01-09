package com.inmobi.databus.datamovement;

import org.apache.log4j.Logger;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class CalendarHelper {
	static Logger logger = Logger.getLogger(CalendarHelper.class);


	public static String getCurrentDayTimeAsPath() {
		Calendar calendar;
		calendar = new GregorianCalendar();
		String year = Integer.toString(calendar.get(Calendar.YEAR));
		String month = Integer.toString(calendar.get(Calendar.MONTH) + 1);
		String day = Integer.toString(calendar.get(Calendar.DAY_OF_MONTH));
		String hour = Integer.toString(calendar.get(Calendar.HOUR_OF_DAY));
		String minute = Integer.toString(calendar.get(Calendar.MINUTE));
		String pathInYYMMDDHRMNFormat = year + "/" + month + "/" +  day + "/" + hour + "/" + minute + "/";

		logger.debug("getCurrentDayTimeAsPath :: Path [" + pathInYYMMDDHRMNFormat + "]");
		return pathInYYMMDDHRMNFormat;

	}

	public static String getCurrentMinute() {
		Calendar calendar;
		calendar = new GregorianCalendar();
		String minute = Integer.toString(calendar.get(Calendar.MINUTE));
		return minute;
	}

	private static String getCurrentDayTimeAsString(boolean includeHourMinute) {
		Calendar calendar;
    String minute = null;
    String hour = null;
    String fileNameInnYYMMDDHRMNFormat = null;
    calendar = new GregorianCalendar();
		String year = Integer.toString(calendar.get(Calendar.YEAR));
		String month = Integer.toString(calendar.get(Calendar.MONTH) + 1);
		String day = Integer.toString(calendar.get(Calendar.DAY_OF_MONTH));
    if (includeHourMinute) {
		 hour = Integer.toString(calendar.get(Calendar.HOUR_OF_DAY));
 		 minute = Integer.toString(calendar.get(Calendar.MINUTE));
    }
		if (includeHourMinute)
     fileNameInnYYMMDDHRMNFormat = year + "-" + month + "-" +  day + "-" + hour + "-" + minute;
    else
    fileNameInnYYMMDDHRMNFormat =  year + "-" + month + "-" +  day;
		logger.debug("getCurrentDayTimeAsString ::  [" + fileNameInnYYMMDDHRMNFormat + "]");
		return fileNameInnYYMMDDHRMNFormat;

	}

  public static String getCurrentDayTimeAsString() {
    return getCurrentDayTimeAsString(true);
  }

  public static String getCurrentDateAsString() {
    return getCurrentDayTimeAsString(false);
  }

}

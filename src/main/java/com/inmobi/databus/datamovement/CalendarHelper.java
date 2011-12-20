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

	public static String getCurrentDayTimeAsString() {
		Calendar calendar;
		calendar = new GregorianCalendar();
		String year = Integer.toString(calendar.get(Calendar.YEAR));
		String month = Integer.toString(calendar.get(Calendar.MONTH) + 1);
		String day = Integer.toString(calendar.get(Calendar.DAY_OF_MONTH));
		String hour = Integer.toString(calendar.get(Calendar.HOUR_OF_DAY));
		String minute = Integer.toString(calendar.get(Calendar.MINUTE));
		String fileNameInnYYMMDDHRMNFormat = year + "-" + month + "-" +  day + "-" + hour + "-" + minute;

		logger.debug("getCurrentDayTimeAsString ::  [" + fileNameInnYYMMDDHRMNFormat + "]");
		return fileNameInnYYMMDDHRMNFormat;

	}

}

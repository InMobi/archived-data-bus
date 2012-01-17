package com.inmobi.databus.utils;

import org.apache.log4j.Logger;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class CalendarHelper {
  static Logger logger = Logger.getLogger(CalendarHelper.class);

  // TODO - all date/time should be returned in a common time zone GMT

  public static String getCurrentDayTimeAsPath() {
    Calendar calendar;
    calendar = new GregorianCalendar();
    String year = Integer.toString(calendar.get(Calendar.YEAR));
    String month = Integer.toString(calendar.get(Calendar.MONTH) + 1);
    String day = Integer.toString(calendar.get(Calendar.DAY_OF_MONTH));
    String hour = Integer.toString(calendar.get(Calendar.HOUR_OF_DAY));
    String minute = Integer.toString(calendar.get(Calendar.MINUTE));
    String pathInYYMMDDHRMNFormat = year + "/" + month + "/" + day + "/" + hour
        + "/" + minute + "/";

    logger.debug("getCurrentDayTimeAsPath :: Path [" + pathInYYMMDDHRMNFormat
        + "]");
    return pathInYYMMDDHRMNFormat;

  }

  public static Calendar getDate(String year, String month, String day) {
    return new GregorianCalendar(new Integer(year).intValue(), new Integer(
        month).intValue() - 1, new Integer(day).intValue());
  }

  public static Calendar getDate(Integer year, Integer month, Integer day) {
    return new GregorianCalendar(year.intValue(), month.intValue() - 1,
        day.intValue());
  }

  public static String getCurrentMinute() {
    Calendar calendar;
    calendar = new GregorianCalendar();
    String minute = Integer.toString(calendar.get(Calendar.MINUTE));
    return minute;
  }

  public static Calendar getNowTime() {
    return new GregorianCalendar();
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
      fileNameInnYYMMDDHRMNFormat = year + "-" + month + "-" + day + "-" + hour
          + "-" + minute;
    else
      fileNameInnYYMMDDHRMNFormat = year + "-" + month + "-" + day;
    logger.debug("getCurrentDayTimeAsString ::  ["
        + fileNameInnYYMMDDHRMNFormat + "]");
    return fileNameInnYYMMDDHRMNFormat;

  }

  public static String getCurrentDayTimeAsString() {
    return getCurrentDayTimeAsString(true);
  }

  public static String getCurrentDateAsString() {
    return getCurrentDayTimeAsString(false);
  }

}

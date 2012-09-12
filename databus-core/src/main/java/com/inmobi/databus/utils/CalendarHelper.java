/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.inmobi.databus.utils;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class CalendarHelper {
  static Logger logger = Logger.getLogger(CalendarHelper.class);

  static String minDirFormatStr = "yyyy" + File.separator + "MM" +
      File.separator + "dd" + File.separator + "HH" + File.separator +"mm";

  static final ThreadLocal<DateFormat> minDirFormat =
      new ThreadLocal<DateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          return new SimpleDateFormat(minDirFormatStr);
        }
      };

  // TODO - all date/time should be returned in a common time zone GMT

  public static Date getDateFromStreamDir(Path streamDirPrefix, Path dir) {
    String pathStr = dir.toString();
    int startIndex = streamDirPrefix.toString().length() + 1;
    /* logger.debug("StartIndex [" + startIndex + "] PathStr [" + pathStr
   +"] endIndex [" +  (startIndex + minDirFormatStr.length()) + "] length ["
   + pathStr.length() +"]");
    */
    String dirString = pathStr.substring(startIndex,
        startIndex + minDirFormatStr.length());
    try {
      return minDirFormat.get().parse(dirString);
    } catch (ParseException e) {
      logger.warn("Could not get date from directory passed", e);
    }
    return null;
  }

  public static Calendar getDate(String year, String month, String day) {
    return new GregorianCalendar(new Integer(year).intValue(), new Integer(
        month).intValue() - 1, new Integer(day).intValue());
  }

  public static Calendar getDate(Integer year, Integer month, Integer day) {
    return new GregorianCalendar(year.intValue(), month.intValue() - 1,
        day.intValue());
  }

  public static Calendar getDateHour(String year, String month, String day,
                                     String hour) {
    return new GregorianCalendar(new Integer(year).intValue(), new Integer(
        month).intValue() - 1, new Integer(day).intValue(),
        new Integer(hour).intValue(), new Integer(0));
  }

  public static Calendar getDateHourMinute(Integer year, Integer month,
                                           Integer day, Integer hour, Integer minute) {
    return new GregorianCalendar(year.intValue(), month.intValue() - 1,
        day.intValue(), hour.intValue(), minute.intValue());
  }

  public static String getCurrentMinute() {
    Calendar calendar;
    calendar = new GregorianCalendar();
    String minute = Integer.toString(calendar.get(Calendar.MINUTE));
    return minute;
  }

  public static String getCurrentHour() {
    Calendar calendar;
    calendar = new GregorianCalendar();
    String hour = Integer.toString(calendar.get(Calendar.HOUR_OF_DAY));
    return hour;
  }

  public static Calendar getNowTime() {
    return new GregorianCalendar();
  }

  private static String getCurrentDayTimeAsString(boolean includeMinute) {
    return getDayTimeAsString(new GregorianCalendar(), includeMinute,
        includeMinute);
  }

  private static String getDayTimeAsString(Calendar calendar,
                                           boolean includeHour,
                                           boolean includeMinute) {
    String minute = null;
    String hour = null;
    String fileNameInnYYMMDDHRMNFormat = null;
    String year = Integer.toString(calendar.get(Calendar.YEAR));
    String month = Integer.toString(calendar.get(Calendar.MONTH) + 1);
    String day = Integer.toString(calendar.get(Calendar.DAY_OF_MONTH));
    if (includeHour || includeMinute) {
      hour = Integer.toString(calendar.get(Calendar.HOUR_OF_DAY));
    }
    if (includeMinute) {
      minute = Integer.toString(calendar.get(Calendar.MINUTE));
    }
    if (includeHour) {
      fileNameInnYYMMDDHRMNFormat = year + "-" + month + "-" + day + "-" + hour;
    } else if (includeMinute) {
      fileNameInnYYMMDDHRMNFormat = year + "-" + month + "-" + day + "-" + hour
          + "-" + minute;
    } else {
      fileNameInnYYMMDDHRMNFormat = year + "-" + month + "-" + day;
    }
    logger.debug("getCurrentDayTimeAsString ::  ["
        + fileNameInnYYMMDDHRMNFormat + "]");
    return fileNameInnYYMMDDHRMNFormat;

  }

  public static String getCurrentDayTimeAsString() {
    return getCurrentDayTimeAsString(true);
  }

  public static String getCurrentDayHourAsString() {
    return getDayTimeAsString(new GregorianCalendar(), true, false);
  }

  public static String getCurrentDateAsString() {
    return getCurrentDayTimeAsString(false);
  }

  public static String getDateAsString(Calendar calendar) {
    return getDayTimeAsString(calendar, false, false);
  }

  public static String getDateTimeAsString(Calendar calendar) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    return format.format(calendar.getTime());
  }

  public static Calendar getDateTime(String dateTime) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    Calendar calendar = new GregorianCalendar();
    try {
      calendar.setTime(format.parse(dateTime));
    } catch(Exception e){
    }
    return calendar;
  }

}

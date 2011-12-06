package com.inmobi.databus;

import org.apache.log4j.Logger;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Created by IntelliJ IDEA.
 * User: inderbir.singh
 * Date: 05/12/11
 * Time: 12:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class CalendarHelper {
    static Logger logger = Logger.getLogger(CalendarHelper.class);

    String year;
    String month;
    String day;
    String hour;
    String minute;

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
}

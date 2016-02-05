package com.nexr.dip.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Utility
 */
public class Utils {

    public static long parseTime(String timeString) throws ParseException {
        String pattern = "yyyy-MM-dd HH:mm:ss,SSS";
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.parse(timeString).getTime();
    }

    public static long parseTime(String timeString, String pattern) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.parse(timeString).getTime();
    }

    public static String formatTime(long timestamp) {
        String pattern = "yyyy-MM-dd HH:mm:ss,SSS";
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        return format.format(calendar.getTime());
    }

    public static String formatTime(long timestamp, String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        return format.format(calendar.getTime());
    }

    public static String formatTime(long timestamp, String pattern, String timezone) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        format.setTimeZone(TimeZone.getTimeZone(timezone));
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp);
        return format.format(calendar.getTime());
    }


    public static String getDateString(long time) {
        return formatTime(time, "yyyy-MM-dd HH:mm:ss");
    }
}

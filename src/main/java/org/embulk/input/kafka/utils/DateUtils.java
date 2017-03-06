package org.embulk.input.kafka.utils;

import org.embulk.input.kafka.exception.DateFormatException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class DateUtils {

  public final static String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public final static String APACHE_DATETIME_FORMAT = "[dd/MMM/yyyy:HH:mm:ss Z]";
  public final static Locale LOCALE = Locale.ENGLISH;

  public static SimpleDateFormat format(String datetime) throws DateFormatException {
    try {
      SimpleDateFormat df = new SimpleDateFormat(DATETIME_FORMAT);
      df.parse(datetime);
      return df;
    } catch (ParseException e) {
    }
    try {
      SimpleDateFormat df = new SimpleDateFormat(APACHE_DATETIME_FORMAT, LOCALE);
      df.parse(datetime);
      return df;
    } catch (ParseException e) {
    }
    throw new DateFormatException("Format doesn't exists");
  }

  public static long getTime(String datetime) throws DateFormatException, ParseException {
    return format(datetime).parse(datetime).getTime();
  }
}

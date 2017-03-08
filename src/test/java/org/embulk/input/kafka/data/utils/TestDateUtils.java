package org.embulk.input.kafka.data.utils;

import org.embulk.input.kafka.exception.DateFormatException;
import org.embulk.input.kafka.utils.DateUtils;
import org.junit.Test;

import java.text.SimpleDateFormat;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class TestDateUtils {
  @Test
  public void format() throws DateFormatException {
    String datetime = "2017-03-08 12:00:00";
    SimpleDateFormat dateFormat = DateUtils.format(datetime);
    assertThat(dateFormat.toPattern(), is(DateUtils.DATETIME_FORMAT));
  }
}

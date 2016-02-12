package com.nexr.camus.etl.kafka.partitioner;

import com.linkedin.camus.etl.kafka.common.EtlKey;
import com.nexr.camus.etl.kafka.partitioner.SimpleDailyPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimpleDailyPartitionerTest {

  @Test
  public void testDefaultConfiguration() throws Exception {
    SimpleDailyPartitioner underTest = new SimpleDailyPartitioner();
    underTest.setConf(configurationFor(null, null));

    long time = new DateTime(2014, 2, 1, 14, 30, 11, 234, DateTimeZone.forID("Asia/Seoul")).getMillis();
    String partition = underTest.encodePartition(null, etlKeyWithTime(time));
    String path = underTest.generatePartitionedPath(null, "tpc", partition);

    assertEquals("tpc/ndap-daily/2014-02-01-00-00-00", path);
  }

  @Test
  public void testPicksUpConfiguration() throws Exception {
    SimpleDailyPartitioner underTest = new SimpleDailyPartitioner();
    underTest.setConf(configurationFor("incoming", "Europe/Amsterdam"));

    //long time = new DateTime(2014, 2, 1, 1, 0, 0, 0, DateTimeZone.forID("Europe/Amsterdam")).getMillis();
    long time = new DateTime(System.currentTimeMillis(), DateTimeZone.forID("Asia/Seoul")).getMillis();
    String partition = underTest.encodePartition(null, etlKeyWithTime(time));
    String path = underTest.generatePartitionedPath(null, "tpc", partition);

    assertEquals("tpc/incoming/2014/02/01", path);
  }

  private EtlKey etlKeyWithTime(long time) {
    EtlKey etlKey = new EtlKey();
    etlKey.setTime(time);
    return etlKey;
  }

  private Configuration configurationFor(String path, String dateTimeZone) {
    Configuration conf = new Configuration();
    if (path != null) conf.set("etl.destination.path.topic.sub.dir", path);
    if (dateTimeZone != null) conf.set("etl.default.timezone", dateTimeZone);
    return conf;
  }

}

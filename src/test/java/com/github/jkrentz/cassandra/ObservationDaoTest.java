package com.github.jkrentz.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.jkrentz.cassandra.Observation;
import com.github.jkrentz.cassandra.ObservationDao;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

public class ObservationDaoTest {
  private static final Logger log = Logger.getLogger(ObservationDaoTest.class);

  public void testDao(ObservationDao dao) throws InterruptedException, ExecutionException {

    UUID sensorId1 = new UUID(0, 100);
    UUID sensorId2 = new UUID(0, 200);

    DateTime baseDate = new DateTime();
    int readingInterval = 1; // 1 second

    List<Observation> readings = new ArrayList<Observation>();

    for (int ii = 0; ii < 10; ii++) {

      BigDecimal temperature = new BigDecimal(230 + ii).movePointLeft(1);
      readings.add(new Observation(sensorId1, baseDate.plusSeconds(readingInterval * ii), temperature));

      temperature = new BigDecimal(195 - ii).movePointLeft(1);
      readings.add(new Observation(sensorId2, baseDate.plusSeconds(readingInterval * ii), temperature));
    }

    dao.save(readings);

    List<Observation> expectedReadings = new ArrayList<Observation>();
    for (int ii = 0; ii < 3; ii++) {
      BigDecimal temperature = new BigDecimal(234 + ii).movePointLeft(1);
      expectedReadings.add(new Observation(sensorId1, baseDate.plusSeconds(readingInterval * (ii + 4)), temperature));
    }

    DateTime startTime = new DateTime(baseDate.plusSeconds(readingInterval * 4));
    DateTime endTime = new DateTime(baseDate.plusSeconds(readingInterval * 6));
    Duration duration = new Duration(startTime, endTime);
    Interval interval = new Interval(startTime, duration);

    Collection<Observation> returnedReadings = dao.query(sensorId1, interval);

    assertEquals(expectedReadings.size(), returnedReadings.size());

    for (Observation expectedReading : expectedReadings) {
      assertTrue(returnedReadings.contains(expectedReading));
    }

    expectedReadings = new ArrayList<Observation>();
    for (int ii = 0; ii < 4; ii++) {
      BigDecimal temperature = new BigDecimal(195 - ii).movePointLeft(1);
      expectedReadings.add(new Observation(sensorId2, baseDate.plusSeconds(readingInterval * ii), temperature));
    }

    startTime = new DateTime(baseDate);
    endTime = new DateTime(baseDate.plusSeconds(readingInterval * 3));
    duration = new Duration(startTime, endTime);
    interval = new Interval(startTime, duration);

    returnedReadings = dao.query(sensorId2, interval);

    assertEquals(expectedReadings.size(), returnedReadings.size());

    for (Observation expectedReading : expectedReadings) {
      assertTrue(returnedReadings.contains(expectedReading));
    }
  }

  public void testPerformance(ObservationDao dao) throws InterruptedException, ExecutionException {

    List<Observation> expected = new ArrayList<Observation>();

    UUID procedureId = new UUID(0, 300);
    Random rand = new Random();

    for (int i = 0; i < 1_000_000; i++) {
      expected.add(new Observation(procedureId, new DateTime(i), new BigDecimal(rand.nextFloat())));
    }
    dao.save(expected);
    List<Observation> results = dao.query(procedureId, new Interval(0, 1_000_000));

    assertEquals(expected.size(), results.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), results.get(i));
    }
  }

}

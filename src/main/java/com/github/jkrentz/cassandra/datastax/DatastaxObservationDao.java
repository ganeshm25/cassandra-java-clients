package com.github.jkrentz.cassandra.datastax;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.jkrentz.cassandra.Observation;
import com.github.jkrentz.cassandra.ObservationDao;

public class DatastaxObservationDao implements ObservationDao {
  private static final Logger log = Logger.getLogger(DatastaxObservationDao.class);
  
  private int SAVE_BATCH_SIZE = 250;
  private int GET_PAGE_SIZE = 100;

  private final Session session;

  // private final PreparedStatement simpleInsertStatement;
  private final PreparedStatement intervalQuery;
  private final PreparedStatement intervalPageQuery;

  private final String table;

  public DatastaxObservationDao(Session session, String keyspaceName, String columnFamily) {
    this.session = session;
    this.table = keyspaceName + "." + columnFamily;

    // simpleInsertStatement = session.prepare("INSERT INTO " + table + " (sensorid, datetime, value) " + " VALUES (?,?,?);");
    intervalQuery = session.prepare("SELECT * FROM " + table + " WHERE sensorid = ? AND datetime >= ? AND datetime <= ? LIMIT " + GET_PAGE_SIZE + ";");
    intervalPageQuery = session.prepare("SELECT * FROM " + table + " WHERE sensorid = ? AND datetime > ? AND datetime <= ? LIMIT " + GET_PAGE_SIZE + ";");
  }
  
  public void appendInsertStatement(StringBuilder builder, Observation observation) {
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append(" (sensorid, datetime, value) VALUES (");
    builder.append(observation.getProcedureId());
    builder.append(",");
    builder.append(observation.getDateTime().getMillis());
    builder.append(",");
    builder.append(observation.getValue());
    builder.append("); ");
  }

  @Override
  public void save(Iterable<Observation> observations) throws InterruptedException, ExecutionException {    
    StringBuilder builder = new StringBuilder("BEGIN UNLOGGED BATCH ");
    int count = 0;
    
    for (Observation observation : observations) {
      appendInsertStatement(builder, observation);
      count++;
      
      if (count >= SAVE_BATCH_SIZE) {
        builder.append("APPLY BATCH;");
        session.execute(builder.toString());
        builder = new StringBuilder("BEGIN UNLOGGED BATCH ");
        count = 0;
      }
      
    }
    
    if (count > 0) {
      builder.append("APPLY BATCH;");
      session.execute(builder.toString());
    }
  }
  
  /*
  @Override
  public void save(Iterable<Observation> observations) throws InterruptedException, ExecutionException {
    BoundStatement statement = new BoundStatement(simpleInsertStatement);
    for (Observation observation : observations) {
      session.execute(
          statement.bind(observation.getProcedureId(), observation.getDateTime().toDate(), observation.getValue())
      );
    }
  }
  */

  @Override
  public List<Observation> query(UUID procedureId, Interval interval) {

    // perform first query using inclusive starting bound
    BoundStatement statement = new BoundStatement(intervalQuery);
    List<Observation> observations = new ArrayList<Observation>();
    DateTime pageStart = interval.getStart();
    ResultSet results = session.execute(statement.bind(procedureId, pageStart.toDate(), interval.getEnd().toDate()));

    for (Row row : results) {
      DateTime dateTime = new DateTime(row.getDate("datetime").getTime());
      BigDecimal value = row.getDecimal("value");

      Observation observation = new Observation(procedureId, dateTime, value);
      observations.add(observation);
    }

    if (observations.size() < GET_PAGE_SIZE) {
      return observations;
    }
    pageStart = observations.get(observations.size() - 1).getDateTime();

    // perform subsequent queries using exclusive starting bound 
    statement = new BoundStatement(intervalPageQuery);
    while (pageStart.isBefore(interval.getEnd())) {
      results = session.execute(statement.bind(procedureId, pageStart.toDate(), interval.getEnd().toDate()));

      int pageResults = 0;
      for (Row row : results) {
        DateTime dateTime = new DateTime(row.getDate("datetime").getTime());
        BigDecimal value = row.getDecimal("value");

        Observation observation = new Observation(procedureId, dateTime, value);
        observations.add(observation);
        pageResults++;
      }

      // there were not enough results to fill a page.
      if (pageResults < GET_PAGE_SIZE) {
        break;
      }

      // the end of this page is the start of the next.
      pageStart = observations.get(observations.size() - 1).getDateTime();
    }

    return observations;
  }

}
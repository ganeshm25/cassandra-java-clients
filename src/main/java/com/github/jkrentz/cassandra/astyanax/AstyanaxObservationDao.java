package com.github.jkrentz.cassandra.astyanax;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.github.jkrentz.cassandra.Observation;
import com.github.jkrentz.cassandra.ObservationDao;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.BigDecimalSerializer;
import com.netflix.astyanax.util.RangeBuilder;

public class AstyanaxObservationDao implements ObservationDao {
  private static final Logger log = Logger.getLogger(AstyanaxObservationDao.class);

  private static final Serializer<BigDecimal> BIGDECIMAL_SERIALIZER = BigDecimalSerializer.get();

  // observations never expire.
  private static final Integer TTL = null;
  private static final int SAVE_BATCHSIZE = 1000;

  private final Keyspace keyspace;
  private final ColumnFamily<UUID, Long> columnFamilyInfo;

  public AstyanaxObservationDao(Keyspace keyspace, ColumnFamily<UUID, Long> columnFamilyInfo) {
    this.keyspace = keyspace;
    this.columnFamilyInfo = columnFamilyInfo;
  }

  @Override
  public void save(Iterable<Observation> observations) throws InterruptedException, ExecutionException {    
    MutationBatch mutationBatch = keyspace.prepareMutationBatch();
    int batchSize = 0;

    for (Observation observation : observations) {
      try {
        mutationBatch.withRow(columnFamilyInfo, observation.getProcedureId()).putColumn(observation.getDateTime().getMillis(), BIGDECIMAL_SERIALIZER.toByteBuffer(observation.getValue()), TTL);
        batchSize++;

        if (batchSize >= SAVE_BATCHSIZE) {
          mutationBatch.execute();
          mutationBatch.discardMutations();
          batchSize = 0;
        }
      } catch (ConnectionException e) {
        throw new RuntimeException("Save failed", e);
      }
    }

    if (batchSize > 0) {
      try {
        mutationBatch.execute();
        mutationBatch.discardMutations();
      } catch (ConnectionException e) {
        throw new RuntimeException("Save failed", e);
      }
    }
  }

  @Override
  public List<Observation> query(UUID procedureId, Interval interval) {
    List<Observation> observations = new ArrayList<Observation>();
    ByteBufferRange range = new RangeBuilder().setStart(interval.getStartMillis()).setEnd(interval.getEndMillis()).build();
    RowQuery<UUID, Long> query = keyspace.prepareQuery(columnFamilyInfo).getKey(procedureId).autoPaginate(true).withColumnRange(range);

    try {
      ColumnList<Long> columns = query.executeAsync().get().getResult();

      for (Column<Long> column : columns) {
        DateTime dateTime = new DateTime(column.getName());
        Observation reading = new Observation(procedureId, dateTime, column.getValue(BIGDECIMAL_SERIALIZER));
        observations.add(reading);
      }
    } catch (ConnectionException | InterruptedException | ExecutionException e) {
      throw new RuntimeException("Query failed", e);
    }

    return observations;
  }
}

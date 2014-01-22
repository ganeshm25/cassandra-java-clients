package com.github.jkrentz.cassandra.hector;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.BigDecimalSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.UUIDSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.github.jkrentz.cassandra.Observation;
import com.github.jkrentz.cassandra.ObservationDao;

public class HectorObservationDao implements ObservationDao {
  private static final Logger log = Logger.getLogger(HectorObservationDao.class);

  private static final Serializer<UUID> UUID_SERIALIZER = UUIDSerializer.get();
  private static final Serializer<Long> LONG_SERIALIZER = LongSerializer.get();
  private static final Serializer<BigDecimal> BIGDECIMAL_SERIALIZER = BigDecimalSerializer.get();

  private static final int SAVE_BATCHSIZE = 100;

  private final Keyspace keyspace;
  private final String columnFamilyName;

  public HectorObservationDao(Keyspace keyspace, String columnFamilyName) {
    this.keyspace = keyspace;
    this.columnFamilyName = columnFamilyName;
  }

  @Override
  public void save(Iterable<Observation> observations) {
    Mutator<UUID> mutator = HFactory.createMutator(keyspace, UUID_SERIALIZER);

    int batchSize = 0;
    for (Observation observation : observations) {
      HColumn<Long, BigDecimal> column = HFactory.createColumn(observation.getDateTime().getMillis(), observation.getValue(), LONG_SERIALIZER, BIGDECIMAL_SERIALIZER);
      mutator.addInsertion(observation.getProcedureId(), columnFamilyName, column);
      
      batchSize++;
      if (batchSize >= SAVE_BATCHSIZE) {
        mutator.execute();
        mutator.discardPendingMutations();
        batchSize = 0;
      }
    }

    if (batchSize > 0) {
      mutator.execute();      
    }
  }

  @Override
  public List<Observation> query(UUID procedureId, Interval interval) {
    SliceQuery<UUID, Long, BigDecimal> query = HFactory.createSliceQuery(keyspace, UUID_SERIALIZER, LONG_SERIALIZER, BIGDECIMAL_SERIALIZER)
        .setColumnFamily(columnFamilyName)
        .setKey(procedureId);
    
    ColumnSliceIterator<UUID, Long, BigDecimal> iterator = new ColumnSliceIterator<UUID, Long, BigDecimal>(query, interval.getStartMillis(), interval.getEndMillis(), false, Integer.MAX_VALUE);

    List<Observation> observations = new ArrayList<Observation>();
    while (iterator.hasNext()) {
      HColumn<Long, BigDecimal> column = iterator.next();

      DateTime dateTime = new DateTime(column.getName());
      Observation reading = new Observation(procedureId, dateTime, column.getValue());
      observations.add(reading);
    }

    return observations;
  }
}

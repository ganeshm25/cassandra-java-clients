package com.github.jkrentz.cassandra;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.joda.time.Interval;

public interface ObservationDao {
    public void save(Iterable<Observation> observations) throws InterruptedException, ExecutionException;
    public List<Observation> query(UUID procedureId, Interval interval);
}

package com.github.jkrentz.cassandra.hector;

import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import static com.github.jkrentz.cassandra.CassandraConnection.*;

public class HectorClient {

	public static Keyspace setupClient() throws Exception {

		CassandraHostConfigurator configurator = new CassandraHostConfigurator(	HOSTNAME + ":" + PORT);
		Cluster cluster = HFactory.getOrCreateCluster(CLUSTER_NAME, configurator);
		Keyspace keyspace = HFactory.createKeyspace(KEYSPACE_NAME, cluster);
		return keyspace;

	}

}

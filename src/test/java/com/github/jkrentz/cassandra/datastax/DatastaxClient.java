package com.github.jkrentz.cassandra.datastax;

import static com.github.jkrentz.cassandra.CassandraConnection.HOSTNAME;
import static com.github.jkrentz.cassandra.CassandraConnection.NATIVE_PORT;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class DatastaxClient {
	
	private static final Logger log = Logger.getLogger(DatastaxClient.class);
	
	public static Session setupClient() {

		Cluster cluster = Cluster.builder()
				.addContactPoint(HOSTNAME)
				.withPort(NATIVE_PORT)
				// .withSSL() // Uncomment if using client to node encryption
				.build();

		Session session = cluster.connect();
		return session;
            
	}

}

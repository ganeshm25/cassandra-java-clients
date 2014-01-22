package com.github.jkrentz.cassandra.astyanax;

import static com.github.jkrentz.cassandra.CassandraConnection.CLUSTER_NAME;
import static com.github.jkrentz.cassandra.CassandraConnection.HOSTNAME;
import static com.github.jkrentz.cassandra.CassandraConnection.KEYSPACE_NAME;
import static com.github.jkrentz.cassandra.CassandraConnection.PORT;

import com.netflix.astyanax.AstyanaxConfiguration;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.ConnectionPoolConfiguration;
import com.netflix.astyanax.connectionpool.ConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class AstyanaxClient {

	public static Keyspace setupClient() {
            
		AstyanaxConfiguration configuration = new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE);

		ConnectionPoolConfiguration connectionPoolConfiguration = new ConnectionPoolConfigurationImpl("AstyanaxPool")
			.setPort(PORT)
			.setMaxConnsPerHost(8)
			.setSeeds(HOSTNAME + ":" + PORT);

		ConnectionPoolMonitor connectionPoolMonitor = new CountingConnectionPoolMonitor();

		ThriftFamilyFactory factory = ThriftFamilyFactory.getInstance();

		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
			.forCluster(CLUSTER_NAME)
			.forKeyspace(KEYSPACE_NAME)
			.withAstyanaxConfiguration(configuration)
			.withConnectionPoolConfiguration(connectionPoolConfiguration)
			.withConnectionPoolMonitor(connectionPoolMonitor)
			.buildKeyspace(factory);

		context.start();

		Keyspace keyspace = context.getClient();

		return keyspace;

	}
}

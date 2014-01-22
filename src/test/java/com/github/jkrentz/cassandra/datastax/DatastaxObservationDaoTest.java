package com.github.jkrentz.cassandra.datastax;

import static com.github.jkrentz.cassandra.CassandraConnection.KEYSPACE_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Session;
import com.github.jkrentz.cassandra.EmbeddedCassandra;
import com.github.jkrentz.cassandra.ObservationDaoTest;

public class DatastaxObservationDaoTest extends DatastaxClient {
	private static final Logger log = Logger.getLogger(DatastaxObservationDaoTest.class);
    
	private static final String columnFamilyName = "observations";
    private static Session session;
    
    @BeforeClass
    public static void configureCassandra() throws Exception {
    	// schema creation statements
    	List<String> cassandraCommands = new ArrayList<String>();
        cassandraCommands.add("create keyspace " + KEYSPACE_NAME + ";");
        
    	new EmbeddedCassandra("cassandra.yaml", cassandraCommands)
    		.init();
        
    	// datastax client connection
        session = setupClient();
        
    	// need to use cql3 to create the table
        session.execute(
				"CREATE TABLE " + KEYSPACE_NAME + "." + columnFamilyName + " (" +
						"sensorid uuid," + 
						"datetime timestamp," + 
						"value decimal," + 
						"PRIMARY KEY ( sensorid, datetime )," +
				") WITH caching = 'none'; ");
    }
    
    @Test
    public void testDatastaxAccess() throws InterruptedException, ExecutionException {
    	DatastaxObservationDao dao = new DatastaxObservationDao(session, KEYSPACE_NAME, columnFamilyName);
    	
    	ObservationDaoTest genericTest = new ObservationDaoTest();
      genericTest.testDao(dao);
      genericTest.testPerformance(dao);
    }
}

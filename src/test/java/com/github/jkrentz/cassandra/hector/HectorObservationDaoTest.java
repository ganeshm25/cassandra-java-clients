package com.github.jkrentz.cassandra.hector;

import static com.github.jkrentz.cassandra.CassandraConnection.KEYSPACE_NAME;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.Keyspace;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.jkrentz.cassandra.EmbeddedCassandra;
import com.github.jkrentz.cassandra.ObservationDaoTest;
import com.github.jkrentz.cassandra.hector.HectorObservationDao;

public class HectorObservationDaoTest extends HectorClient {
    private static final Logger log = Logger.getLogger(HectorObservationDaoTest.class);

    private static final String columnFamilyName = "observations";
    protected static Keyspace keyspace;

    @BeforeClass
    public static void setup() throws Exception {
    	
    	// schema creation statements
    	List<String> cassandraCommands = new ArrayList<String>();
        cassandraCommands.add("create keyspace " + KEYSPACE_NAME + ";");
        cassandraCommands.add("use " + KEYSPACE_NAME + ";");
        cassandraCommands.add("create column family " + columnFamilyName + " with caching = none;");
        
    	new EmbeddedCassandra("cassandra.yaml", cassandraCommands)
    		.init();

        keyspace = setupClient();
    }

    @Test
    public void testHectorAccess() throws Exception {
    	HectorObservationDao dao = new HectorObservationDao(keyspace, columnFamilyName);
    	
    	ObservationDaoTest genericTest = new ObservationDaoTest();
        genericTest.testDao(dao);
        genericTest.testPerformance(dao);
    }
}

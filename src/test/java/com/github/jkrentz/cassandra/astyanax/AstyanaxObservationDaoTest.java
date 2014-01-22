package com.github.jkrentz.cassandra.astyanax;

import static com.github.jkrentz.cassandra.CassandraConnection.KEYSPACE_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.jkrentz.cassandra.EmbeddedCassandra;
import com.github.jkrentz.cassandra.ObservationDaoTest;
import com.github.jkrentz.cassandra.astyanax.AstyanaxObservationDao;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.BigDecimalSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;

public class AstyanaxObservationDaoTest extends AstyanaxClient {
    private static final Logger log = Logger.getLogger(AstyanaxObservationDaoTest.class);

    private static final String columnFamilyName = "observations";
    private static Keyspace keyspace;
    private static ColumnFamily<UUID, Long> ASTYANAX_CF;

    @BeforeClass
    public static void setup() throws Exception {
    	
    	// schema creation statements
    	List<String> cassandraCommands = new ArrayList<String>();
        cassandraCommands.add("create keyspace " + KEYSPACE_NAME + ";");
        cassandraCommands.add("use " + KEYSPACE_NAME + ";");
        cassandraCommands.add("create column family " + columnFamilyName + " with caching = none;");
        
    	new EmbeddedCassandra("cassandra.yaml", cassandraCommands)
    		.init();
        
    	// astyanax client connection
        keyspace = setupClient();
        
        // long life reference object
        ASTYANAX_CF = new ColumnFamily<UUID, Long>(columnFamilyName,
                UUIDSerializer.get(),   	// Key Serializer
                LongSerializer.get(),		// Column Serializer
                BigDecimalSerializer.get());  	// Value Serializer
    }

    @Test
    public void testAstyanaxAccess() throws InterruptedException, ExecutionException {
        AstyanaxObservationDao dao = new AstyanaxObservationDao(keyspace, ASTYANAX_CF);
        
        ObservationDaoTest genericTest = new ObservationDaoTest();
        genericTest.testDao(dao);
        genericTest.testPerformance(dao);
    }
}

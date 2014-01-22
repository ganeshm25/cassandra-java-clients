package com.github.jkrentz.cassandra;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.junit.Test;

import com.github.jkrentz.cassandra.EmbeddedCassandra;

public class EmbeddedCassandraTest {

    private static final String embeddedCassandraKeySpaceName = "TestKeyspace";
    private static final String columnFamilyName = "TestColumnFamily";
    private static final String cassandraYaml = "cassandra.yaml";

    @Test
    public void testEmbedded() throws URISyntaxException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException, InstantiationException, IOException, TException, TimedOutException, NotFoundException, InvalidRequestException, UnavailableException {
        List<String> cassandraCommands = new ArrayList<String>();
        cassandraCommands.add("create keyspace " + embeddedCassandraKeySpaceName + ";");
        cassandraCommands.add("use " + embeddedCassandraKeySpaceName + ";");
        cassandraCommands.add("create column family " + columnFamilyName + ";");

        EmbeddedCassandra embeddedCassandra = new EmbeddedCassandra(cassandraYaml, cassandraCommands);
        embeddedCassandra.init();
        assertNotNull(embeddedCassandra);
    }
}

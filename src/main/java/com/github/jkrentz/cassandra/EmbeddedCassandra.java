package com.github.jkrentz.cassandra;

import static org.apache.commons.io.FileUtils.copyFileToDirectory;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.commons.io.FileUtils.forceMkdir;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.cli.CliMain;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class EmbeddedCassandra {

  private static final String HOSTNAME = "localhost";
  private static final int PORT = 9161;
  private static final boolean CLEAN = true;
  private static final String CONFIG_DIRECTORY = "target/cassandra";

  private List<String> startupCommands;
  private String configFilename;
  private File configFile;

  private static final Logger log = Logger.getLogger(EmbeddedCassandra.class);

  public EmbeddedCassandra(String cassandraYaml, List<String> startupCommands) {
    this.configFilename = cassandraYaml;
    this.startupCommands = startupCommands;
  }

  public void init() throws IOException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException, InstantiationException, TException, TimedOutException, NotFoundException, InvalidRequestException, UnavailableException, URISyntaxException {
    URL stream = EmbeddedCassandra.class.getClassLoader().getResource(configFilename);
    configFile = new File(stream.toURI());

    setupStorageConfigPath();
    if (CLEAN)
      clean();

    EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
    cassandra.start();
    executeCommands();
  }

  private void setupStorageConfigPath() throws IOException {
    File configFile = new File(CONFIG_DIRECTORY);
    String configFileName = "file:" + configFile.getPath() + "/" + configFilename;
    System.setProperty("cassandra.config", configFileName);
  }

  private void executeCommands() throws CharacterCodingException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException, InstantiationException, TException, TimedOutException, NotFoundException, InvalidRequestException, UnavailableException {

    // not interested in the cli output
    if (log.getLevel() != Level.DEBUG) {
      CliMain.sessionState.setOut(new PrintStream(new NullOutputStream()));
    }

    CliMain.connect(HOSTNAME, PORT);

    for (String command : startupCommands) {
      CliMain.processStatement(command);
    }

    CliMain.disconnect();
  }

  private void clean() throws IOException {
    cleanupDataDirectories();
    initializeCassandraYaml();
    makeDirsIfNotExist();
  }

  private void cleanupDataDirectories() throws IOException {
    deleteDirectory(new File(CONFIG_DIRECTORY));
  }

  private void initializeCassandraYaml() throws IOException {
    copyFileToDirectory(configFile, new File(CONFIG_DIRECTORY));
  }

  private void makeDirsIfNotExist() throws IOException {
    for (String s : Arrays.asList(DatabaseDescriptor.getAllDataFileLocations())) {
      forceMkdir(new File(s));
    }
    forceMkdir(new File(DatabaseDescriptor.getCommitLogLocation()));
  }
}

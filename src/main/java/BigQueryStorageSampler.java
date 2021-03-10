import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.base.Stopwatch;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class BigQueryStorageSampler {

  private static class TableReference {

    static Pattern tableReferencePattern =
        Pattern.compile("([\\w\\-]+)[\\:\\.]([^\\:\\.]+)\\.([^\\:\\.]+)");

    static TableReference parseFromString(String table) {
      Matcher m = tableReferencePattern.matcher(table);
      if (!m.matches()) {
        throw new IllegalArgumentException(table + " is not a valid table reference");
      }

      TableReference tableReference = new TableReference();
      tableReference.project = m.group(1);
      tableReference.dataset = m.group(2);
      tableReference.table = m.group(3);
      return tableReference;
    }

    private String project;
    private String dataset;
    private String table;

    public String toResourceName() {
      return "projects/" + this.project + "/datasets/" + this.dataset + "/tables/" + this.table;
    }
  }

  static class ProjectReference {

    static ProjectReference parseFromString(String project) {
      ProjectReference projectReference = new ProjectReference();
      projectReference.project = project;
      return projectReference;
    }

    private String project;

    public String toResourceName() {
      return "projects/" + this.project;
    }
  }

  static class DataFormat {

    static DataFormat parseFromString(String format) {
      DataFormat dataFormat = new DataFormat();
      switch (format.toLowerCase()) {
        case "avro":
          dataFormat.dataFormat = com.google.cloud.bigquery.storage.v1.DataFormat.AVRO;
          break;
        case "arrow":
          dataFormat.dataFormat = com.google.cloud.bigquery.storage.v1.DataFormat.ARROW;
          break;
        default:
          throw new IllegalArgumentException("Unknown data format " + format);
      }

      return dataFormat;
    }

    com.google.cloud.bigquery.storage.v1.DataFormat dataFormat;

    public com.google.cloud.bigquery.storage.v1.DataFormat toProto() {
      return this.dataFormat;
    }
  }

  static Options getParseOptions() {
    Options parseOptions = new Options();
    parseOptions.addOption(
        Option.builder("p")
            .longOpt("parent")
            .desc("The ID of the parent project for the read session")
            .required()
            .hasArg()
            .type(String.class)
            .build());
    parseOptions.addOption(
        Option.builder("t")
            .longOpt("table")
            .desc("The fully-qualified ID of the table to read from")
            .required()
            .hasArg()
            .type(String.class)
            .build());
    parseOptions.addOption(
        Option.builder("f")
            .longOpt("format")
            .desc("The format of the data (Avro or Arrow)")
            .required()
            .hasArg()
            .type(String.class)
            .build());
    parseOptions.addOption(
        Option.builder("s")
            .longOpt("streams")
            .desc("The number of streams to request during session creation")
            .hasArg()
            .type(Integer.class)
            .build());
    parseOptions.addOption(
        Option.builder()
            .longOpt("endpoint")
            .desc("The read API endpoint for the operation")
            .hasArg()
            .type(String.class)
            .build());
    parseOptions.addOption(
        Option.builder()
            .longOpt("protocol")
            .desc("The transport protocol to use for the ReadRows call")
            .hasArg()
            .type(String.class)
            .build());
    return parseOptions;
  }

  private static BigQueryReadClient getClient(Optional<String> endpoint, Optional<String> protocol) throws Exception {
    BigQueryReadSettings.Builder builder = BigQueryReadSettings.newBuilder();
    endpoint.ifPresent(s -> builder.setEndpoint(s + ":443"));
    protocol.ifPresent(s -> builder.setHeaderProvider(
        FixedHeaderProvider.create("x-bigquerystorage-transport-protocol", s)));
    return BigQueryReadClient.create(builder.build());
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = parser.parse(getParseOptions(), args);

    TableReference tableReference =
        TableReference.parseFromString(commandLine.getOptionValue("table"));
    ProjectReference parentProjectReference =
        ProjectReference.parseFromString(commandLine.getOptionValue("parent"));
    DataFormat dataFormat =
        DataFormat.parseFromString(commandLine.getOptionValue("format"));
    int streams = Integer.parseInt(commandLine.getOptionValue("streams", "1"));
    Optional<String> endpoint =
        Optional.ofNullable(commandLine.getOptionValue("endpoint"));
    Optional<String> protocol =
        Optional.ofNullable(commandLine.getOptionValue("protocol"));

    System.out.println("Table: " + tableReference.toResourceName());
    System.out.println("Parent: " + parentProjectReference.toResourceName());
    System.out.println("Data format: " + commandLine.getOptionValue("format"));

    CreateReadSessionRequest createReadSessionRequest =
        CreateReadSessionRequest.newBuilder()
            .setParent(parentProjectReference.toResourceName())
            .setReadSession(
                ReadSession.newBuilder()
                    .setTable(tableReference.toResourceName())
                    .setDataFormat(dataFormat.toProto()))
            .setMaxStreamCount(streams)
            .build();

    ReadSession readSession;
    long elapsedMillis;
    try (BigQueryReadClient client = getClient(endpoint, protocol)) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      readSession = client.createReadSession(createReadSessionRequest);
      stopwatch.stop();
      elapsedMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    System.out.println("Created read session " + readSession.getName());
    double displaySeconds = elapsedMillis / 1000.0;
    System.out.println("Read session creation took " + displaySeconds + " seconds");

    String streamName = readSession.getStreams(0).getName();

    ReadRowsRequest readRowsRequest =
        ReadRowsRequest.newBuilder().setReadStream(streamName).build();

    long numResponses = 0;
    long numRows = 0;
    long numTotalBytes = 0;
    long lastReportTimeNanos = 0;

    try (BigQueryReadClient client = getClient(endpoint, protocol)) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      for (ReadRowsResponse  response : client.readRowsCallable().call(readRowsRequest)) {
        numResponses++;
        numRows += response.getRowCount();
        numTotalBytes += response.getSerializedSize();

        long elapsedTimeNanos = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        if (elapsedTimeNanos - lastReportTimeNanos >= TimeUnit.SECONDS.toNanos(10)) {
          System.out.println(String.format(
              "Received %d responses (%d rows) from stream %s in 10s (%f MB/s)",
              numResponses, numRows, streamName, (double) numTotalBytes / (1024 * 1024 * 10)));

          numResponses = 0;
          numRows = 0;
          numTotalBytes = 0;
          lastReportTimeNanos = elapsedTimeNanos;
        }

        // This is just a simple end-to-end throughput test, so we don't decode the Avro record
        // block here. Note this may well have an impact on throughput in a normal use case!
      }

      stopwatch.stop();
      long elapsedTimeNanos = stopwatch.elapsed(TimeUnit.NANOSECONDS);
      if (elapsedTimeNanos - lastReportTimeNanos >= TimeUnit.SECONDS.toNanos(10)) {
        System.out.println(String.format(
            "Received %d responses (%d rows) from stream %s in 10s (%f MB/s)",
            numResponses, numRows, streamName, (double) numTotalBytes / (1024 * 1024 * 10)));

        numResponses = 0;
        numRows = 0;
        numTotalBytes = 0;
        lastReportTimeNanos = elapsedTimeNanos;
      }

      System.out.println("Finished reading from stream " + streamName);
    }
  }
}

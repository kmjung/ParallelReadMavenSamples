import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.common.base.Stopwatch;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

public class BigQueryStorageSampler {

  private static class TableReference {

    static Pattern tableReferencePattern = Pattern.compile("([\\w\\-]+)[\\:\\.]([^\\:\\.]+)\\.([^\\:\\.]+)");

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

  static class ReaderThread extends Thread {

    final ReadStream readStream;
    final BigQueryReadClient client;

    long numResponses = 0;
    long numResponseBytes = 0;
    long numResponseRows = 0;
    long lastReportTimeMicros = 0;

    long numTotalResponses = 0;
    long numTotalResponseBytes = 0;
    long numTotalResponseRows = 0;

    private Schema avroSchema;
    private GenericDatumReader<GenericRecord> datumReader;

    private VectorSchemaRoot arrowRoot;
    private RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);

    public ReaderThread(ReadStream readStream, BigQueryReadClient client) {
      this.readStream = readStream;
      this.client = client;
    }

    public void run() {
      try {
        readRows();
      } catch (Exception e) {
        System.err.println("Caught exception while calling ReadRows: " + e);
      } finally {
        if (arrowRoot != null) {
          arrowRoot.close();
        }
        rootAllocator.close();
      }
    }

    private void readRows() throws Exception {
      ReadRowsRequest readRowsRequest = ReadRowsRequest.newBuilder().setReadStream(readStream.getName()).build();
      Stopwatch stopwatch = Stopwatch.createStarted();
      for (ReadRowsResponse response : client.readRowsCallable().call(readRowsRequest)) {
        numResponses++;
        numResponseBytes += response.getSerializedSize();
        numResponseRows += response.getRowCount();
        printPeriodicUpdate(stopwatch.elapsed(TimeUnit.MICROSECONDS));

        // Initialize data schema.
        // The schema is only sent in the first response.
        if (response.hasAvroSchema()) {
          if (avroSchema == null) {
            avroSchema = new Schema.Parser().parse(response.getAvroSchema().getSchema());
            datumReader = new GenericDatumReader<>(avroSchema);
          }
        } else if (response.hasArrowSchema()) {
          if (arrowRoot == null) {
            ByteArrayInputStream schemaStream = new ByteArrayInputStream(
                response.getArrowSchema().getSerializedSchema().toByteArray());
            try (ArrowStreamReader reader = new ArrowStreamReader(schemaStream, rootAllocator)) {
              arrowRoot = reader.getVectorSchemaRoot();
            }
          }
        }

        if (datumReader != null && response.hasAvroRows()) {
          BinaryDecoder decoder = DecoderFactory.get()
              .binaryDecoder(
                  response.getAvroRows().getSerializedBinaryRows().toByteArray(), null);
          while (!decoder.isEnd()) {
            GenericRecord record = datumReader.read(null, decoder);
            System.out.println(record.toString());
          }
        } else if (arrowRoot != null && response.hasArrowRecordBatch()) {
          byte[] batchBytes = response.getArrowRecordBatch().getSerializedRecordBatch().toByteArray();
          try (final ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(
              new ReadChannel(Channels.newChannel(new ByteArrayInputStream(batchBytes))),
              rootAllocator)) {
            VectorLoader loader = new VectorLoader(arrowRoot);
            loader.load(batch);
            for (int i = 0; i < arrowRoot.getRowCount(); i++) {
              StringBuilder sb = new StringBuilder();
              sb.append("{");
              for (int j = 0; j < arrowRoot.getFieldVectors().size(); j++) {
                if (j > 0) {
                  sb.append(", ");
                }
                sb.append(arrowRoot.getSchema().getFields().get(j).getName());
                sb.append(": ");
                sb.append(arrowRoot.getFieldVectors().get(j).getObject(i));
              }
              sb.append("}");
              System.out.println(sb.toString());
            }
          }
        }
      }

      stopwatch.stop();
      updateCumulativeStatistics();
      System.out.println("Finished reading from stream " + readStream.getName());
    }

    private void printPeriodicUpdate(long elapsedMicros) {
      if (elapsedMicros - lastReportTimeMicros < TimeUnit.SECONDS.toMicros(10)) {
        return;
      }

      System.out.printf(
          "Received %d responses (%d rows) from stream %s in 10s (%f MiB/s)%n",
          numResponses, numResponseRows, readStream.getName(),
          (double) numResponseBytes / (1024 * 1024 * 10));

      updateCumulativeStatistics();
      lastReportTimeMicros = elapsedMicros;
    }

    private void updateCumulativeStatistics() {
      numTotalResponses += numResponses;
      numResponses = 0;
      numTotalResponseBytes += numResponseBytes;
      numResponseBytes = 0;
      numTotalResponseRows += numResponseRows;
      numResponseRows = 0;
    }

    private long getNumTotalResponseRows() {
      return numTotalResponseRows;
    }
  }

  private static BigQueryReadClient getClient(BigQueryStorageSamplerOptions options) throws IOException {
    ChannelPoolSettings.Builder channelPoolSettingsBuilder = ChannelPoolSettings.builder();
    options.getChannelPoolOptions().getMinChannelCount().ifPresent(channelPoolSettingsBuilder::setMinChannelCount);
    options.getChannelPoolOptions().getMaxChannelCount().ifPresent(channelPoolSettingsBuilder::setMaxChannelCount);
    options.getChannelPoolOptions().getMinRpcsPerChannel().ifPresent(channelPoolSettingsBuilder::setMinRpcsPerChannel);
    options.getChannelPoolOptions().getMaxRpcsPerChannel().ifPresent(channelPoolSettingsBuilder::setMaxRpcsPerChannel);
    options.getChannelPoolOptions().getInitialChannelCount().ifPresent(
        channelPoolSettingsBuilder::setInitialChannelCount);

    // TODO(kmj): Can we fetch this directly from the stub settings rather than
    // copying it?
    InstantiatingGrpcChannelProvider.Builder channelProviderBuilder = InstantiatingGrpcChannelProvider.newBuilder()
        .setMaxInboundMessageSize(Integer.MAX_VALUE)
        .setChannelPoolSettings(channelPoolSettingsBuilder.build());

    // Note: this setter method overrides the channel pool settings specified above.
    options.getChannelsPerCpu().ifPresent(channelProviderBuilder::setChannelsPerCpu);

    options.getFlowControlWindowSize().ifPresent(s -> channelProviderBuilder.setChannelConfigurator(
        (ManagedChannelBuilder channelBuilder) -> {
          if (channelBuilder instanceof NettyChannelBuilder) {
            System.out.println("Setting flow control window size to " + s);
            return ((NettyChannelBuilder) channelBuilder).flowControlWindow(s);
          } else {
            System.err.println("Failed to set flow control window size!");
            return channelBuilder;
          }
        }));

    BigQueryReadSettings.Builder settingsBuilder = BigQueryReadSettings.newBuilder()
        .setTransportChannelProvider(channelProviderBuilder.build());

    options.getEndpoint().ifPresent(settingsBuilder::setEndpoint);
    options.getExecutorThreadCount().ifPresent(s -> settingsBuilder.setBackgroundExecutorProvider(
        InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(s).build()));

    return BigQueryReadClient.create(settingsBuilder.build());
  }

  public static void main(String[] args) throws Exception {
    BigQueryStorageSamplerOptions options = new BigQueryStorageSamplerOptions(args);

    System.out.println("Parent project: " + options.getParent());
    System.out.println("Table: " + options.getTable());
    System.out.println("Data format: " + options.getFormat());

    CreateReadSessionRequest createReadSessionRequest = CreateReadSessionRequest.newBuilder()
        .setParent(
            ProjectReference.parseFromString(options.getParent()).toResourceName())
        .setReadSession(
            ReadSession.newBuilder()
                .setTable(
                    TableReference.parseFromString(options.getTable()).toResourceName())
                .setDataFormat(
                    DataFormat.parseFromString(options.getFormat()).toProto()))
        .setMaxStreamCount(options.getMaxStreams())
        .build();

    ReadSession readSession;
    long elapsedMillis;
    try (BigQueryReadClient client = getClient(options)) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      readSession = client.createReadSession(createReadSessionRequest);
      stopwatch.stop();
      elapsedMillis = stopwatch.elapsed(TimeUnit.MILLISECONDS);

      System.out.println("Created read session " + readSession.getName());
      double displaySeconds = elapsedMillis / 1000.0;
      System.out.println("Read session creation took " + displaySeconds + " seconds");

      List<ReaderThread> readerThreads = new ArrayList<>(readSession.getStreamsCount());
      for (ReadStream readStream : readSession.getStreamsList()) {
        System.out.println("Creating a reader thread for stream " + readStream.getName());
        readerThreads.add(new ReaderThread(readStream, client));
      }

      for (ReaderThread readerThread : readerThreads) {
        readerThread.start();
      }

      for (ReaderThread readerThread : readerThreads) {
        readerThread.join();
      }

      long numTotalResponseRows = 0;
      for (ReaderThread readerThread : readerThreads) {
        numTotalResponseRows += readerThread.getNumTotalResponseRows();
      }

      System.out.println("All reader threads finished after reading " + numTotalResponseRows + " rows; exiting");
    }
  }
}

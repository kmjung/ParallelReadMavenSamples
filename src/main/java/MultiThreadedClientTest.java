import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.DataFormat;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1beta1.Storage.StreamPosition;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto.TableReference;
import com.google.common.base.Stopwatch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiThreadedClientTest {

  private static final Integer DEFAULT_NUM_THREADS = 4;

  private static final TableReference TABLE_REFERENCE = TableReference.newBuilder()
      .setProjectId("bigquery-public-data")
      .setDatasetId("samples")
      .setTableId("wikipedia")
      .build();

  private static class ReaderThread extends Thread {

    final Stream stream;

    long numResponses = 0;
    long numRows = 0;
    long numTotalBytes = 0;
    long lastReportTimeNanos = 0;

    public ReaderThread(Stream stream) {
      this.stream = stream;
    }

    public void run() {
      try {
        readRows();
      } catch (Exception e) {
        System.err.println(String.format("Caught %s while calling ReadRows; exiting", e));
      }
    }

    private void readRows() throws Exception {

      ReadRowsRequest request =
          ReadRowsRequest.newBuilder()
              .setReadPosition(StreamPosition.newBuilder().setStream(stream).setOffset(0))
              .build();

      try (BigQueryStorageClient client = BigQueryStorageClient.create()) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (ReadRowsResponse response : client.readRowsCallable().call(request)) {
          numResponses++;
          numRows += response.getAvroRows().getRowCount();
          numTotalBytes += response.getSerializedSize();
          printPeriodicUpdate(stopwatch.elapsed(TimeUnit.NANOSECONDS));

          // This is just a simple end-to-end throughput test, so we don't decode the Avro record
          // block here. Note this may well have an impact on throughput in a normal use case!
        }

        stopwatch.stop();
        printPeriodicUpdate(stopwatch.elapsed(TimeUnit.NANOSECONDS));
        System.out.println("Finished reading from stream " + stream.getName());
      }
    }

    private void printPeriodicUpdate(long elapsedTimeNanos) {
      if (elapsedTimeNanos - lastReportTimeNanos >= TimeUnit.SECONDS.toNanos(10)) {
        System.out.println(String.format(
            "Received %d responses (%d rows) from stream %s in 10s (%f MB/s)",
            numResponses, numRows, stream.getName(), (double) numTotalBytes / (1024 * 1024 * 10)));

        numResponses = 0;
        numRows = 0;
        numTotalBytes = 0;
        lastReportTimeNanos = elapsedTimeNanos;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      throw new IllegalArgumentException("Parent project name is required");
    }

    String parent = String.format("projects/%s", args[0]);
    int numThreads = (args.length > 1) ? Integer.parseInt(args[1]) : DEFAULT_NUM_THREADS;

    CreateReadSessionRequest request =
        CreateReadSessionRequest.newBuilder()
            .setTableReference(TABLE_REFERENCE)
            .setFormat(DataFormat.AVRO)
            .setParent(parent)
            .setRequestedStreams(numThreads)
            .build();

    List<ReaderThread> readerThreads = new ArrayList<>();

    try (BigQueryStorageClient client = BigQueryStorageClient.create()) {
      ReadSession readSession = client.createReadSession(request);
      System.out.println(String.format("Created read session with ID %s", readSession.getName()));
      for (Stream stream : readSession.getStreamsList()) {
        System.out.println(
            String.format("Creating a new reader thread for stream %s", stream.getName()));
        readerThreads.add(new ReaderThread(stream));
      }
    }

    for (ReaderThread readerThread : readerThreads) {
      readerThread.start();
    }

    for (ReaderThread readerThread : readerThreads) {
      readerThread.join();
    }

    System.out.println("All reader threads finished; exiting");
  }
}

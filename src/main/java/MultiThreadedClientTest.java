import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1alpha1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1alpha1.Storage.StreamPosition;
import com.google.cloud.bigquery.v3.TableReferenceProto.TableReference;
import com.google.common.base.Stopwatch;
import io.opencensus.common.Scope;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiThreadedClientTest {

  private static final TableReference TABLE_REFERENCE = TableReference.newBuilder()
      .setProjectId("bigquery-public-data")
      .setDatasetId("samples")
      .setTableId("wikipedia")
      .build();

  private static final int NUM_THREADS = 4;

  private static void startReader(int threadId, Stream stream) throws Exception {

    StreamPosition streamPosition = StreamPosition.newBuilder()
        .setStream(stream)
        .setOffset(0)
        .build();

    ReadRowsRequest request = ReadRowsRequest.newBuilder()
        .setReadPosition(streamPosition)
        .build();

    long numResponses = 0;
    long numRows = 0;
    long numTotalBytes = 0;
    long lastReportTimeNanos = 0;

    try (BigQueryStorageClient client = BigQueryStorageClient.create()) {
      Stopwatch stopwatch = Stopwatch.createStarted();
      for (ReadRowsResponse response : client.readRowsCallable().call(request)) {
        numResponses++;
        numRows += response.getRowsCount();
        numTotalBytes += response.getSerializedSize();

        long elapsedTimeNanos = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        if (elapsedTimeNanos - lastReportTimeNanos > TimeUnit.SECONDS.toNanos(10)) {
          System.out.println(String.format(
              "Thread %d received %d responses (%d rows) in 10s (%f MB/s)",
              threadId, numResponses, numRows, (double) numTotalBytes / (1024 * 1024 * 10)));
          numResponses = numRows = numTotalBytes = 0;
          lastReportTimeNanos = elapsedTimeNanos;
        }

        if (elapsedTimeNanos > TimeUnit.SECONDS.toNanos(60)) {
          break;
        }
      }
    }

    System.out.println("Thread " + threadId + " done");
  }

  public static void main(String[] args) throws Exception {
    ReadSession readSession;
    try (BigQueryStorageClient client = BigQueryStorageClient.create()) {
      readSession = client.createReadSession(TABLE_REFERENCE, NUM_THREADS);
    }

    System.out.println("Created session with ID " + readSession.getName());

    int numStreams = readSession.getStreamsCount();
    ExecutorService executorService = Executors.newFixedThreadPool(numStreams);
    for (int i = 0; i < numStreams; i++) {
      final int thread_id = i;
      executorService.submit(() -> {
        try {
          startReader(thread_id, readSession.getStreams(thread_id));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }

    executorService.shutdown();
    executorService.awaitTermination(30, TimeUnit.MINUTES);
    System.out.println("Work items completed; exiting");
  }
}

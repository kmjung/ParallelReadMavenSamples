import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.v3.ParallelRead.ReadLocation;
import com.google.cloud.bigquery.v3.ParallelRead.ReadOptions;
import com.google.cloud.bigquery.v3.ParallelRead.ReadRowsRequest;
import com.google.cloud.bigquery.v3.ParallelRead.ReadRowsResponse;
import com.google.cloud.bigquery.v3.ParallelRead.Session;
import com.google.cloud.bigquery.v3.ParallelReadServiceClient;
import com.google.cloud.bigquery.v3.ParallelReadServiceSettings;
import com.google.cloud.bigquery.v3.TableReferenceProto.TableReference;
import com.google.common.base.Stopwatch;
import io.opencensus.common.Scope;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;

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

  private static final Tracer tracer = Tracing.getTracer();

  private static void startReader(int threadId, ReadLocation readLocation) throws Exception {

    ReadRowsRequest request = ReadRowsRequest.newBuilder()
        .setReadLocation(readLocation)
        .setOptions(ReadOptions.newBuilder().setMaxRows(1000))
        .build();

    long numResponses = 0;
    long numRows = 0;
    long numTotalBytes = 0;
    long lastReportTimeNanos = 0;

    try (Scope ss = tracer.spanBuilder("thread-" + threadId).startScopedSpan()) {
      try (ParallelReadServiceClient client = ParallelReadServiceClient.create()) {
        ServerStream<ReadRowsResponse> stream = client.readRowsCallable().call(request);
        tracer.getCurrentSpan().addAnnotation("Created read stream " + threadId);
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (ReadRowsResponse response : stream) {
          tracer.getCurrentSpan().addAnnotation("Received ReadRowsResponse");

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
    }

    System.out.println("Thread " + threadId + " done");
  }

  public static void main(String[] args) throws Exception {

    StackdriverTraceExporter.createAndRegister(
        StackdriverTraceConfiguration.builder().build());

    Session session;
    try (ParallelReadServiceClient client = ParallelReadServiceClient.create()) {
      session = client.createSession(TABLE_REFERENCE, NUM_THREADS);
    }

    System.out.println("Created session with ID " + session.getName());

    int numReaders = session.getInitialReadLocationsCount();
    ExecutorService executorService = Executors.newFixedThreadPool(numReaders);
    for (int i = 0; i < numReaders; i++) {
      final int thread_id = i;
      executorService.submit(() -> {
        try {
          startReader(thread_id, session.getInitialReadLocations(thread_id));
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

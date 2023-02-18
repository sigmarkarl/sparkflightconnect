package org.simmi;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.api.python.Py4JServer;
import org.apache.spark.api.r.RAuthHelper;
import org.apache.spark.api.r.RBackend;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class SparkFlightConnect implements AutoCloseable {
    //10.71.73.177
    //final static  Location          location = Location.forGrpcInsecure("sparkflightserver.default", 33333);
    final static  Location          location = Location.forGrpcInsecure("127.0.0.1", 33333);
    private       Py4JServer        py4jServer;
    private       RBackend          rBackend;
    private       SparkAppStreaming sparkAppStreaming;
    private final ExecutorService   executor;
    private       Future<String>    streamRes;
    private       int               rbackendPort;
    private       String            rbackendSecret;

    public SparkFlightConnect() {
        executor = Executors.newSingleThreadExecutor();
    }

    private void init() {
        sparkAppStreaming = new SparkAppStreaming();
        initPy4JServer(sparkAppStreaming.getSparkSession());
        initRBackend();
        startJupyterProcess();
        sparkAppStreaming.setPy4JServer(py4jServer);
        sparkAppStreaming.setRBackendSecret(rbackendSecret);
        sparkAppStreaming.setRBackendPort(rbackendPort);
        streamRes = executor.submit(sparkAppStreaming);
    }

    private static Optional<String> getJupyterBaseUrl() {
        var baseurl = System.getenv("JUPYTER_BASE_URL");
        if (baseurl == null) {
            baseurl = System.getProperty("JUPYTER_BASE_URL");
        }
        if (baseurl != null && !baseurl.isEmpty()) {
            return Optional.of(baseurl);
        }
        return Optional.empty();
    }

    private void startJupyterProcess() {
        var plist = new ArrayList<>(
                List.of("jupyter-lab", "--ip=0.0.0.0", "--NotebookApp.allow_origin='*'", "--port=8889",
                        "--NotebookApp.port_retries=0",
                        "--no-browser")); //, "--NotebookApp.token","''","--NotebookApp.disable_check_xsrf","True"));
        var notebookdir = System.getenv("JUPYTER_NOTEBOOK_DIR");
        if (notebookdir == null) {
            notebookdir = System.getProperty("JUPYTER_NOTEBOOK_DIR");
        }
        if (notebookdir != null && !notebookdir.isEmpty()) {
            plist.add("--notebook-dir=" + notebookdir);
        }
        var baseurlOpt = getJupyterBaseUrl();
        if (baseurlOpt.isPresent()) {
            var baseurl = baseurlOpt.get();
            plist.add("--NotebookApp.base_url=/" + baseurl);
            plist.add("--LabApp.base_url=/" + baseurl);
        }

        var pyServerPort   = Integer.toString(py4jServer.getListeningPort());
        var pyServerSecret = py4jServer.secret();
        System.err.println(pyServerPort + ";" + pyServerSecret);

        ProcessBuilder pb = new ProcessBuilder(plist);
        pb.inheritIO();
        //standaloneRoot.filter(p -> !p.startsWith("s3")).ifPresent(sroot -> pb.directory(Paths.get(sroot).toFile()));
        Map<String, String> env = pb.environment();
        env.put("PYSPARK_GATEWAY_PORT", pyServerPort);
        env.put("PYSPARK_GATEWAY_SECRET", pyServerSecret);
        env.put("PYSPARK_PIN_THREAD", "true");
        if (rbackendPort > 0) {
            env.put("SPARKR_WORKER_PORT", String.valueOf(rbackendPort));
            env.put("SPARKR_WORKER_SECRET", rbackendSecret);
            env.put("EXISTING_SPARKR_BACKEND_PORT", String.valueOf(rbackendPort));
            env.put("SPARKR_BACKEND_AUTH_SECRET", rbackendSecret);
        }
        try {
            pb.start();
        }
        catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    private void initPy4JServer(SparkSession spark) {
        py4jServer = new Py4JServer(spark.sparkContext()
                                         .conf());
        py4jServer.start();
    }

    private void initRBackend() {
        rBackend = new RBackend();
        Tuple2<Object, RAuthHelper> tuple = rBackend.init();
        rbackendPort = (Integer) tuple._1;
        rbackendSecret = tuple._2.secret();

        //"library(SparkR, lib.loc = c(file.path(Sys.getenv(\"SPARK_HOME\"), \"R\", \"lib\")))" +
        //"sparkR.session()");

        new Thread(() -> rBackend.run()).start();
    }

    public static FlightClient globalClient;

    public static void main(String[] args) throws Exception {
        try (var sparkFlightConnect = new SparkFlightConnect();
             BufferAllocator allocator = new RootAllocator()) {
            sparkFlightConnect.init();
            try (FlightClient flightClient = FlightClient.builder(allocator, location)
                                                         .build()) {
                System.err.println("C1: Client (Location): Connected to " + location.getUri());

                // Populate data
                //Schema schema = new Schema(Arrays.asList(
                //        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

                // Get metadata information
                //FlightInfo flightInfo = flightClient.getInfo(FlightDescriptor.path("profiles"));
                //System.out.println("C3: Client (Get Metadata): " + flightInfo);

                //flightClient.
                // Get data information
                var jsc     = sparkFlightConnect.sparkAppStreaming.getJavaSparkContext();
                var running = true;
                while (running) {
                    try (FlightStream flightStream = flightClient.getStream(new Ticket(FlightDescriptor.path("profiles")
                                                                                                       .getPath()
                                                                                                       .get(0)
                                                                                                       .getBytes(
                                                                                                               StandardCharsets.UTF_8)))) {
                        int batch = 0;
                        globalClient = flightClient;
                        try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
                            System.err.println("C4: Client (Get Stream):");
                            while (flightStream.next()) {
                                batch++;
                                System.err.println("Client Received batch #" + batch + ", Data:");
                                var pythonCodeList = Collections.singletonList(
                                        vectorSchemaRootReceived.getFieldVectors()
                                                                .stream()
                                                                .map(i -> StandardCharsets.UTF_8.decode(
                                                                                                  i.getDataBuffer()
                                                                                                   .nioBuffer())
                                                                                                .toString())
                                                                .map(UTF8String::fromString)
                                                                .collect(Collectors.toList()));
                                var javaRDD = jsc.parallelize(pythonCodeList)
                                                 .map(s -> {
                                                     //var utf8Str = UTF8String.fromString(s);
                                                     var seq = JavaConverters.asScalaIteratorConverter(s.iterator())
                                                                             .asScala()
                                                                             .toSeq();
                                                     var oseq = (Seq<Object>) (Seq) seq;
                                                     return InternalRow.apply(oseq);
                                                 });

                                var ds = sparkFlightConnect.sparkAppStreaming.getSparkSession()
                                                                             .internalCreateDataFrame(javaRDD.rdd(),
                                                                                                      SparkAppStreamSource.SCHEMA,
                                                                                                      true);
                                try {
                                    System.err.println("before queue");
                                    SparkFlightSource.queue.put(ds);
                                    System.err.println("after queue");
                                }
                                catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                    catch (Exception e) {
                        Thread.sleep(60000);
                        e.printStackTrace();
                    }
                }

                // Get all metadata information
                /*Iterable<FlightInfo> flightInfosBefore = flightClient.listFlights(Criteria.ALL);
                System.out.print("C5: Client (List Flights Info): ");
                flightInfosBefore.forEach(System.out::println);

                // Do delete action
                Iterator<Result> deleteActionResult = flightClient.doAction(new Action("DELETE",
                                                                                       FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)));
                while (deleteActionResult.hasNext()) {
                    Result result = deleteActionResult.next();
                    System.out.println("C6: Client (Do Delete Action): " +
                                       new String(result.getBody(), StandardCharsets.UTF_8));
                }

                // Get all metadata information (to validate detele action)
                Iterable<FlightInfo> flightInfos = flightClient.listFlights(Criteria.ALL);
                flightInfos.forEach(t -> System.out.println(t));
                System.out.println("C7: Client (List Flights Info): After delete - No records");*/
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        sparkAppStreaming.close();
        py4jServer.shutdown();
        executor.shutdown();
    }
}
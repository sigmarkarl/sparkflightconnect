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

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class SparkFlightConnect implements AutoCloseable {
    //10.71.73.177
    final static Location          location = Location.forGrpcInsecure("10.71.73.177", 33333);
    private      Py4JServer        py4jServer;
    private      RBackend          rBackend;
    private      SparkAppStreaming sparkAppStreaming;
    private      ExecutorService executor;
    private      Future<String>  streamRes;
    private      int             rbackendPort;
    private      String          rbackendSecret;

    public SparkFlightConnect() {
        executor = Executors.newSingleThreadExecutor();
    }

    private void init() {
        sparkAppStreaming = new SparkAppStreaming();
        initPy4JServer(sparkAppStreaming.getSparkSession());
        initRBackend();
        sparkAppStreaming.setPy4JServer(py4jServer);
        sparkAppStreaming.setRBackendSecret(rbackendSecret);
        sparkAppStreaming.setRBackendPort(rbackendPort);
        streamRes = executor.submit(sparkAppStreaming);
    }

    private void initPy4JServer(SparkSession spark) {
        py4jServer = new Py4JServer(spark.sparkContext().conf());
        py4jServer.start();
    }

    private void initRBackend() {
        rBackend = new RBackend();
        Tuple2<Object, RAuthHelper> tuple = rBackend.init();
        rbackendPort = (Integer)tuple._1;
        rbackendSecret = tuple._2.secret();

                           //"library(SparkR, lib.loc = c(file.path(Sys.getenv(\"SPARK_HOME\"), \"R\", \"lib\")))" +
                           //"sparkR.session()");

        new Thread(() -> rBackend.run()).start();
    }

    public static FlightClient globalClient;

    public static void main(String[] args) throws Exception {
        try (var sparkFlightConnect = new SparkFlightConnect(); BufferAllocator allocator = new RootAllocator()) {
            sparkFlightConnect.init();
            try (FlightClient flightClient = FlightClient.builder(allocator, location).build()) {
                System.err.println("C1: Client (Location): Connected to " + location.getUri());

                // Populate data
                //Schema schema = new Schema(Arrays.asList(
                //        new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));

                // Get metadata information
                //FlightInfo flightInfo = flightClient.getInfo(FlightDescriptor.path("profiles"));
                //System.out.println("C3: Client (Get Metadata): " + flightInfo);

                //flightClient.
                // Get data information
                var jsc = sparkFlightConnect.sparkAppStreaming.getJavaSparkContext();
                try (FlightStream flightStream = flightClient.getStream(new Ticket(
                        FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)))) {
                    int batch = 0;
                    globalClient = flightClient;
                    try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
                        System.err.println("C4: Client (Get Stream):");
                        while (flightStream.next()) {
                            batch++;
                            System.err.println("Client Received batch #" + batch + ", Data:");
                            var pythonCodeList = Collections.singletonList(vectorSchemaRootReceived.getFieldVectors().stream()
                                                                                                   .map(i -> StandardCharsets.UTF_8.decode(i.getDataBuffer().nioBuffer()).toString())
                                                                                                   .map(UTF8String::fromString)
                                                                                                   .collect(
                                                                                                           Collectors.toList()));
                            var javaRDD = jsc.parallelize(pythonCodeList).map(s -> {
                                //var utf8Str = UTF8String.fromString(s);
                                var seq = JavaConverters.asScalaIteratorConverter(s.iterator()).asScala().toSeq();
                                var oseq = (Seq<Object>)(Seq)seq;
                                return InternalRow.apply(oseq);
                            });

                            var ds = sparkFlightConnect.sparkAppStreaming.getSparkSession().internalCreateDataFrame(javaRDD.rdd(), SparkAppStreamSource.SCHEMA, true);
                            try {
                                System.err.println("before queue");
                                SparkFlightSource.queue.put(ds);
                                System.err.println("after queue");
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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
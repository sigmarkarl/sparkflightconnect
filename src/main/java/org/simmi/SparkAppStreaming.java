package org.simmi;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.python.Py4JServer;
import org.apache.spark.api.r.RBackend;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.time.Duration;
import java.util.concurrent.Callable;

public class SparkAppStreaming implements Callable<String>, AutoCloseable {
    private static final int DEFAULT_TIMEOUT = 3600*2;
    private final SparkSession sparkSession;
    private Py4JServer py4JServer;
    private String rBackendSecret;
    private int rBackendPort;
    private JavaSparkContext jsc;

    public SparkAppStreaming(SparkSession sparkSession, Py4JServer py4JServer, String rBackendSecret, int rBackendPort) {
        this.sparkSession = sparkSession;
        this.py4JServer = py4JServer;
        this.rBackendSecret = rBackendSecret;
        this.rBackendPort = rBackendPort;
        this.jsc = new JavaSparkContext(sparkSession.sparkContext());
    }

    public JavaSparkContext getJavaSparkContext() {
    	return jsc;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public SparkAppStreaming() {
        this(SparkSession.builder().master("local[*]")
            .appName("SparkAppStreaming")
        //.config("spark.pyspark.python","/usr/bin/python3")
            //.config("spark.pyspark.virtualenv.enabled",true)
            //.config("spark.pyspark.virtualenv.bin.path","/workspaces/SparkFlightConnect/venv")
            .getOrCreate(), null, null, 0);
    }

    public void setPy4JServer(Py4JServer py4JServer) {
        this.py4JServer = py4JServer;
    }

    public void setRBackendSecret(String rBackendSecret) {
    	this.rBackendSecret = rBackendSecret;
    }

    public void setRBackendPort(int rBackendPort) {
    	this.rBackendPort = rBackendPort;
    }

    @Override
    public String call() throws Exception {
        try(SparkBatchConsumer sparkBatchConsumer = new SparkBatchConsumer(sparkSession, py4JServer.secret(), py4JServer.getListeningPort(), rBackendSecret, rBackendPort)) {
            StreamingQuery query = sparkSession.readStream().format("org.simmi.SparkAppStreamSource")
                                               //.option("stream.keys", streamKey)
                                               .schema(SparkAppStreamSource.SCHEMA)
                                               .load().writeStream().outputMode("update")
                                               .foreachBatch(sparkBatchConsumer).start();
            query.awaitTermination(Duration.ofMinutes(DEFAULT_TIMEOUT).toMillis());
        }
        return "";
    }

    @Override
    public void close() throws Exception {

    }
}

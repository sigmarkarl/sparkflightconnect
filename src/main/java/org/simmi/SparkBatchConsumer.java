package org.simmi;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SparkBatchConsumer implements VoidFunction2<Dataset<Row>, Long>, AutoCloseable {
    private final SparkSession sparkSession;
    private final int port;
    private final String secret;
    private final int rport;
    private final String rsecret;
    private       ExecutorService es = Executors.newFixedThreadPool(2);

    ByteArrayOutputStream baOutput = new ByteArrayOutputStream();
    ByteArrayOutputStream  baError  = new ByteArrayOutputStream();
    public SparkBatchConsumer(SparkSession sparkSession, String secret, int port, String rsecret, int rport) {
        this.sparkSession = sparkSession;
        this.secret = secret;
        this.port = port;
        this.rsecret = rsecret;
        this.rport = rport;
    }

    private void runWasm(String query) {
        var df = sparkSession.sql(query);
        var cl = df.toArrowBatchRdd().toJavaRDD().mapPartitions(new WasmFunction()).collect();
        System.err.println(String.join(",", cl));

        //var wasm = new Wasm();
        //wasm.runWasm(query);
    }

    private void runPython(String query) throws IOException, InterruptedException {
        Files.writeString(Path.of("cmd.py"), query);
        List<String> cmds          = new ArrayList<>();
        var          pysparkPython = System.getenv("PYSPARK_PYTHON");
        cmds.add(pysparkPython != null ? pysparkPython : "python3");
        cmds.add("cmd.py");
        ProcessBuilder      pb  = new ProcessBuilder(cmds);
        Map<String, String> env = pb.environment();

        env.put("PYSPARK_GATEWAY_PORT", Integer.toString(port));
        env.put("PYSPARK_GATEWAY_SECRET", secret);
        env.put("PYSPARK_PIN_THREAD", "true");

        var pythonProcess = pb.start();

        es.submit(() -> pythonProcess.getErrorStream()
                                     .transferTo(System.out));
        es.submit(() -> pythonProcess.getInputStream()
                                     .transferTo(System.err));

        pythonProcess.waitFor();
    }

    private void runRscript(String query) throws IOException, InterruptedException {
        Files.writeString(Path.of("cmd.R"), query);
        List<String> cmds          = new ArrayList<>();
        cmds.add("Rscript");
        cmds.add("cmd.R");
        ProcessBuilder      pb  = new ProcessBuilder(cmds);
        Map<String, String> env = pb.environment();

        env.put("SPARK_HOME", "/opt/homebrew/Cellar/apache-spark/3.3.1/libexec");
        env.put("EXISTING_SPARKR_BACKEND_PORT", Integer.toString(rport));
        env.put("SPARKR_BACKEND_AUTH_SECRET", rsecret);

        var RProcess = pb.start();

        es.submit(() -> RProcess.getErrorStream()
                                     .transferTo(System.out));
        es.submit(() -> RProcess.getInputStream()
                                     .transferTo(System.err));
        RProcess.waitFor();
    }

    @Override
    public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
        List<Row> rows = rowDataset.collectAsList();
        //rowDataset.toArrowBatchRdd();
        for (Row row : rows) {
            var type = row.getString(0);
            var query = row.getString(1);
            var config = row.getString(2);

            sparkSession.sparkContext().setJobGroup("some group", type, false);

            if (type.equals("wasm")) {
                runWasm(query);
            } else if (type.equals("python")) {
                runPython(query);
            } else if(type.equals("R")) {
                runRscript(query);
            } else if(type.equals("sql")) {
                var df = sparkSession.sql(query);
                if (df.isStreaming()) {
                    df.writeStream().format("app").start();
                } else {
                    var l = df.collectAsList();
                    l.forEach(System.err::println);
                    System.err.println(df.schema().treeString());
                    System.err.println(df.count());
                    //df.write().format("app").mode(SaveMode.Append).save();
                    //df.arro
                }
                /*df.writeStream().format("app").foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
                    @Override
                    public void call(Dataset<Row> v1, Long v2) throws Exception {

                    }
                });*/
            }
        }
    }

    @Override
    public void close() {
        sparkSession.close();
        es.shutdown();
    }
}

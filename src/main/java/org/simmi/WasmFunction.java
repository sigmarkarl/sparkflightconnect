package org.simmi;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.execution.arrow.ArrowConverters;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.io.ByteSequence;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WasmFunction implements FlatMapFunction<Iterator<byte[]>, String> {
    static String c_code = """
            int test(int val) {
                return 11;
            }
            """;

    public WasmFunction() {
    }

    public static void test() throws IOException {
        //var reader = new StringReader(c_code);
        var path = Path.of("/Users/sigmar/Documents/GitHub/bundle.so");
        if (Files.exists(path) && Files.size(path) > 0) {
            var sourcebuilder = Source.newBuilder("llvm", path.toFile());
            var source = sourcebuilder.build();
            try(Context polyglot = Context.newBuilder().
                    allowAllAccess(true).build()) {
                var val = polyglot.eval(source);
                var res = val.invokeMember("test1", 10);
                //var res = val2.execute(0, "simmi");
                System.err.println(res);
            }
        } else {
            System.err.println("not exists");
        }
    }

    public static void main(String[] args) {
        try {
            test();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<String> call(Iterator<byte[]> iterator) throws Exception {
        BufferAllocator allocator = new RootAllocator();
        var list = new java.util.ArrayList<String>();
        while (iterator.hasNext()) {
            var                  bytes = iterator.next();
            //ArrowColumnarBatch   batch = new ArrowColumnarBatch(bytes);
            //var arrowRecordBatch = ArrowConverters.loadBatch(bytes, allocator);
            var in = new ByteArrayInputStream(bytes);
            var arrowRecordBatch = MessageSerializer.deserializeRecordBatch(
                    new ReadChannel(Channels.newChannel(in)), allocator);

            System.err.println(arrowRecordBatch.toString());
        }
        return list.iterator();
    }
}

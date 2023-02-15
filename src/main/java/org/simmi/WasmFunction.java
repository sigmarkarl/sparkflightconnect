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
import org.graalvm.polyglot.Value;
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
        //var path = Path.of("/Users/sigmar/Documents/GitHub/bundle.wasm");
        var path = Path.of("/Users/sigmar/Documents/GitHub/polarust3/target/wasm32-wasi/release/polarust3.wasm");
        if (Files.exists(path) && Files.size(path) > 0) {
            var wasmBytes = Files.readAllBytes(path);
            //var sourcebuilder = Source.newBuilder("wasm", path.toFile());
            var sourcebuilder = Source.newBuilder("wasm", ByteSequence.create(wasmBytes), "test");
            var source = sourcebuilder.build();
            try(Context context = Context
                    //.newBuilder()
                    //.allowAllAccess(true)
                    .newBuilder("wasm")
                    .option("wasm.Builtins","wasi_snapshot_preview1")
                    //.arguments("wasm",new String[] {"", "10"})
                    .build()) {
                //context.initialize("wasm");
                var val = context.eval(source);
                //var webAssembly = context.getPolyglotBindings().getMember("WebAssembly").as(WASMModule.class);
                //var mainModule = webAssembly.module_decode(source);

                // create java->WASM bindings and instantiate module
                //Value memory = webAssembly.mem_alloc(108, 1000);
                //Bindings bindings = new Bindings(memory, gameWindow::drawImage);
                //var doomWASM = webAssembly.module_instantiate(mainModule, Value.asValue(bindings)).as(TestWasm.class);


                var test1 = context.getBindings("wasm").getMember("main").getMember("_start");
                var ten = test1.execute();
                //var has = val.mem
                //var res = val.execute();
                //var res = val.invokeMember("w2c_test1", 10);
                //var res = val2.execute(0, "simmi");
                System.err.println(ten);
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

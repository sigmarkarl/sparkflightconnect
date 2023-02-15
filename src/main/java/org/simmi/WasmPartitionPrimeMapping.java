package org.simmi;

import org.apache.hadoop.shaded.com.google.common.collect.Streams;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.ByteSequence;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class WasmPartitionPrimeMapping implements MapPartitionsFunction<Long,Boolean> {

    Context context = null;
    Value isProb = null;
    ByteBuffer byteBuffer = null;

    @Override
    public Iterator<Boolean> call(Iterator<Long> input) throws Exception {
        if (context == null) {
            var path = Path.of("/Users/sigmar/Documents/GitHub/polarust3/target/wasm32-wasi/release/polarust3.wasm");
            var wasmBytes = Files.readAllBytes(path);
            //var sourcebuilder = Source.newBuilder("wasm", path.toFile());
            var sourcebuilder = Source.newBuilder("js", ByteSequence.create(wasmBytes), "test");
            var source = sourcebuilder.build();
            context = Context
                    //.newBuilder()
                    //.allowAllAccess(true)
                    .newBuilder("wasm")
                    .option("wasm.Builtins", "wasi_snapshot_preview1")
                    //.arguments("wasm",new String[] {"", "10"})
                    .build();
            //context.initialize("wasm");
            var val = context.eval(source);
            isProb = val.getMember("isProbablyPrime");
            //val.getMemberKeys().forEach(System.err::println);
            var main = context.getBindings("wasm").getMember("main").getMember("_start");
            var ten = main.execute();
            byteBuffer = ByteBuffer.allocateDirect(8000);
        }
        //context.getMemberKeys().forEach(System.err::println);

        /*return Streams.stream(input)
                .mapMulti((BiConsumer<Long, Consumer<Value>>) (aLong, consumer) -> {
                    if (byteBuffer.limit() == byteBuffer.capacity()) {
                        var res = isProb.execute(byteBuffer);
                        consumer.accept(res);
                        byteBuffer.reset();
                    }
                    byteBuffer.putDouble(aLong);
                })
                .flatMap(l -> Stream.<Boolean>empty())
                .iterator();
        while(input.hasNext()) {
            var nxt = input.next();
            if (byteBuffer.limit() == byteBuffer.capacity()) {
                var res = isProb.execute(byteBuffer.);
                byteBuffer.reset();
            }
            byteBuffer.putDouble(nxt);
        }
        /*if (longBuffer.limit() > 0) {

        }*/
        return Collections.emptyIterator();
    }
}

package org.simmi;

import org.apache.spark.api.java.function.MapFunction;
import org.graalvm.nativeimage.c.type.CDoublePointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.ByteSequence;
import org.graalvm.word.PointerBase;

import java.nio.file.Files;
import java.nio.file.Path;

public class WasmPrimeMapping implements MapFunction<Long,Boolean> {

    Context context = null;
    Value isProb = null;
    Value getBuffer = null;

    @Override
    public Boolean call(Long value) throws Exception {
        if (context == null) {
            var path = Path.of("/Users/sigmar/Documents/GitHub/polarust3/target/wasm32-wasi/release/polarust3.wasm");
            var wasmBytes = Files.readAllBytes(path);
            //var sourcebuilder = Source.newBuilder("wasm", path.toFile());
            var sourcebuilder = Source.newBuilder("wasm", ByteSequence.create(wasmBytes), "test");
            var source = sourcebuilder.build();
            context = Context
                    //.newBuilder()
                    //.allowAllAccess(true)
                    .newBuilder("wasm")
                    .option("wasm.Builtins", "wasi_snapshot_preview1")
                    //.arguments("wasm",new String[] {"", "10"})
                    .build();
            //context.
            //context.initialize("wasm");
            var val = context.eval(source);
            isProb = val.getMember("isProbablyPrime");
            getBuffer = val.getMember("getBuffer");
            //val.getMemberKeys().forEach(System.err::println);
            var main = context.getBindings("wasm").getMember("main").getMember("_start");
            var ten = main.execute();
        }
        //context.getMemberKeys().forEach(System.err::println);
        var res = isProb.execute(value);
        var buf = getBuffer.execute();
        long ptr = buf.asNativePointer();

        //CTypeConversion.asByteBuffer(
        return res.asInt() != 0;
    }
}

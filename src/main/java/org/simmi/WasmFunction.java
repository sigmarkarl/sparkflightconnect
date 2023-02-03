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

import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WasmFunction implements FlatMapFunction<Iterator<byte[]>, String> {

    public WasmFunction() {
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

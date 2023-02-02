package org.simmi;

import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.*;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

import java.io.Serializable;
import java.util.Arrays;

public class AppWrite implements Write, Serializable {

    public BatchWrite toBatch() {
        Schema schema = new Schema(Arrays.asList(
                new Field("letters", FieldType.nullable(new ArrowType.Utf8()), null)));
        try(BufferAllocator allocator = new RootAllocator(); VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
            VarCharVector varCharVectorType = (VarCharVector) vectorSchemaRoot.getVector("letters")) {
            vectorSchemaRoot.setRowCount(1);
            varCharVectorType.allocateNew(1);
            var listener = SparkFlightConnect.globalClient.startPut(FlightDescriptor.path("letters"), vectorSchemaRoot, new AsyncPutListener());
            return new SparkAppBatchWrite(listener, varCharVectorType);
        }
    }

    /**
     * Returns a {@link StreamingWrite} to write data to streaming source. By default this method
     * throws exception, data sources must overwrite this method to provide an implementation, if the
     * {@link Table} that creates this write returns {@link TableCapability#STREAMING_WRITE} support
     * in its {@link Table#capabilities()}.
     */
    public StreamingWrite toStreaming() {
        throw new UnsupportedOperationException(description() + ": Streaming write is not supported");
    }
}

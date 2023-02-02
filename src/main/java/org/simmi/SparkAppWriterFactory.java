package org.simmi;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.vector.VarCharVector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;

public class SparkAppWriterFactory implements DataWriterFactory {
    public VarCharVector varCharVectorType;
    public FlightClient.ClientStreamListener listener;
    public SparkAppWriterFactory(VarCharVector varCharVectorType, FlightClient.ClientStreamListener listener) {
        this.varCharVectorType = varCharVectorType;
        this.listener = listener;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new SparkAppDataWriter(listener, varCharVectorType);
    }
}

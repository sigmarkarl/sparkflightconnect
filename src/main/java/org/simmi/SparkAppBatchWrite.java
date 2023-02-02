package org.simmi;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.sql.connector.write.*;


public class SparkAppBatchWrite implements BatchWrite {
    FlightClient.ClientStreamListener listener;
    VarCharVector varCharVectorType;

    public SparkAppBatchWrite(FlightClient.ClientStreamListener listener, VarCharVector varCharVectorType) {
        this.listener = listener;
        this.varCharVectorType = varCharVectorType;
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new SparkAppWriterFactory(varCharVectorType, listener);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        System.err.println("outer commit");
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        System.err.println("outer abort");
    }
}

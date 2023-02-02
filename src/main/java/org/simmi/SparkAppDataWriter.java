package org.simmi;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.vector.VarCharVector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;

import java.io.IOException;

public class SparkAppDataWriter implements DataWriter<InternalRow> {
    private final FlightClient.ClientStreamListener listener;
    private final VarCharVector                     varCharVectorType;

    public SparkAppDataWriter(FlightClient.ClientStreamListener listener, VarCharVector varCharVectorType) {
        this.listener = listener;
        this.varCharVectorType = varCharVectorType;
    }

    @Override
    public void write(InternalRow record) throws IOException {
        var res = record.getString(0);
        varCharVectorType.set(0, res.getBytes());
        listener.putNext();
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        listener.completed();
        return new WriterCommitMessage() {
        };
    }

    @Override
    public void abort() throws IOException {
        System.err.println("abort");
    }

    @Override
    public void close() throws IOException {
        System.err.println("close");
    }
}

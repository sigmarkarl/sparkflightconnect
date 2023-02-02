package org.simmi;

import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SparkAppSource implements DataSourceRegister, SupportsWrite {
    public static final Set<TableCapability> CAPABILITIES = new HashSet<>(Arrays.asList(
            //TableCapability.BATCH_READ,
            TableCapability.BATCH_WRITE,
            TableCapability.ACCEPT_ANY_SCHEMA
            //TableCapability.TRUNCATE
            // TableCapability.OVERWRITE_BY_FILTER, TableCapability.OVERWRITE_DYNAMIC
            // TableCapability.STREAMING_WRITE, TableCapability.STREAMING_READ,
            // TableCapability.ACCEPT_ANY_SCHEMA, TableCapability.BATCH_READ,
            // TableCapability.BATCH_WRITE, TableCapability.OVERWRITE_BY_FILTER,
            // TableCapability.OVERWRITE_DYNAMIC, TableCapability.TRUNCATE,
            // TableCapability.V1_BATCH_READ, TableCapability.V1_BATCH_WRITE,
            // TableCapability.V1_STREAMING_READ, TableCapability.V1_STREAMING_WRITE,
            // TableCapability.V2_BATCH_READ, TableCapability.V2_BATCH_WRITE,
            // TableCapability.V2_STREAMING_READ, TableCapability.V2_STREAMING_WRITE,
            // TableCapability.V3_BATCH_READ, TableCapability.V3_BATCH_WRITE,
            // TableCapability.V3_STREAMING_READ, TableCapability.V3_STREAMING_WRITE
                                                                                       ));

    @Override
    public String shortName() {
        return "app";
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new AppWriteBuilder();
    }

    @Override
    public String name() {
        return "app";
    }

    @Override
    public StructType schema() {
        return StructType.fromDDL("letters STRING");
    }

    @Override
    public Set<TableCapability> capabilities() {
        return CAPABILITIES;
    }
}

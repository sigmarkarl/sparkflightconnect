package org.simmi;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Source;
import org.apache.spark.sql.sources.StreamSourceProvider;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.Map;

public class SparkAppStreamSource implements StreamSourceProvider {
    private static final StructField[]  fields = {StructField.apply("type", DataTypes.StringType, true, Metadata.empty()), StructField.apply("query", DataTypes.StringType, true, Metadata.empty()), StructField.apply("config", DataTypes.StringType, true, Metadata.empty())};
    public static final StructType SCHEMA = new StructType(fields);

    @Override
    public Tuple2<String, StructType> sourceSchema(SQLContext sqlContext, Option<StructType> schema,
                                                   String providerName, Map<String, String> parameters) {
        return new Tuple2<>("app", SCHEMA);
    }

    @Override
    public Source createSource(SQLContext sqlContext, String metadataPath, Option<StructType> schema,
                               String providerName, Map<String, String> parameters) {
        return new SparkFlightSource(SCHEMA);
    }
}

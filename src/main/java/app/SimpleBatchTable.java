package app;

import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.simmi.AppWriteBuilder;
import org.simmi.SparkAppSource;

import java.util.Set;

public class SimpleBatchTable extends Table implements SupportsWrite, SupportsRead {

    private static final StructField[] fields = {StructField.apply("letters", DataTypes.StringType, true, Metadata.empty())};
    public static final StructType     RESSCHEMA = new StructType(fields);

    public SimpleBatchTable(String name, String database, String description,
                            String tableType, boolean isTemporary) {
        super(name, database, description, tableType, isTemporary);
    }
    /*override def name(): String = "SimpleBatchTable"

    override def schema(): StructType = {
        StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType)
        ))
    }

    override def capabilities(): util.Set[TableCapability] = {
        Set(TableCapability.BATCH_READ).asJava
    }

    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new SimpleScanBuilder()
    }*/

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return null;
    }

    @Override
    public StructType schema() {
        return RESSCHEMA;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return SparkAppSource.CAPABILITIES;
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new AppWriteBuilder();
    }

    @Override
    public Transform[] partitioning() {
        return new Transform[] {
                Expressions.identity("i")
        };
    }

    @Override
    public String name() {
        return "letters";
    }
}

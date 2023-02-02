package app;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class DefaultSource implements TableProvider {

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return SimpleBatchTable.RESSCHEMA;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new SimpleBatchTable("letters", null, null, "table", false);
    }
}

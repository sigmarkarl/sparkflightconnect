package org.simmi;

import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteBuilder;

public class AppWriteBuilder implements WriteBuilder {
    @Override
    public Write build() {
        return new AppWrite();
    }
}

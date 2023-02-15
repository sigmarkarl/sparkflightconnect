package org.simmi;

import org.apache.spark.api.java.function.FilterFunction;

public class BooleanFilter implements FilterFunction<Boolean> {
    @Override
    public boolean call(Boolean value) throws Exception {
        return value;
    }
}

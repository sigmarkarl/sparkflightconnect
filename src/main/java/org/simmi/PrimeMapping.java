package org.simmi;

import org.apache.spark.api.java.function.MapFunction;

import java.math.BigInteger;

public class PrimeMapping implements MapFunction<Long,Boolean> {
    @Override
    public Boolean call(Long value) throws Exception {
        return BigInteger.valueOf(value).isProbablePrime(5);
    }
}

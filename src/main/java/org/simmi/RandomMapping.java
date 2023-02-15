package org.simmi;

import org.apache.spark.api.java.function.MapFunction;

import java.util.Random;

public class RandomMapping implements MapFunction<Long, Long> {
    static Random rnd = new Random();
    @Override
    public Long call(Long value) throws Exception {
        return Math.abs(rnd.nextLong());
    }
}

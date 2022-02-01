/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geops.distributed.spark.util;

import org.apache.spark.util.AccumulatorV2;

/**
 *
 * 
 */
public class MaxLongAccumulator extends AccumulatorV2<Long, Long> {

    long max = Long.MIN_VALUE;

    @Override
    public boolean isZero() {
        return (max == Long.MIN_VALUE);
    }

    @Override
    public AccumulatorV2<Long, Long> copy() {
        MaxLongAccumulator result = new MaxLongAccumulator();

        result.max = this.max;

        return result;
    }

    @Override
    public void reset() {
        this.max = Long.MIN_VALUE;
    }

    @Override
    public void add(Long in) {
        this.max = Long.max(this.max, in);
    }

    @Override
    public void merge(AccumulatorV2<Long, Long> av) {
        try {
            MaxLongAccumulator other = (MaxLongAccumulator) av;
            this.max = Long.max(this.max, other.max);
        } catch (ClassCastException ex) {
            throw new UnsupportedOperationException(String.format("Cannot merge %s with %s", this.getClass().getName(), av.getClass().getName()));
        }
    }

    @Override
    public Long value() {
        return this.max;
    }
}

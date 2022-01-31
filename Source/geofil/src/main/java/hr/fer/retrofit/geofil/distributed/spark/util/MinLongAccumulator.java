/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.distributed.spark.util;

import org.apache.spark.util.AccumulatorV2;

/**
 *
 * 
 */
public class MinLongAccumulator extends AccumulatorV2<Long, Long> {

    long min = Long.MAX_VALUE;

    @Override
    public boolean isZero() {
        return (min == Long.MAX_VALUE);
    }

    @Override
    public AccumulatorV2<Long, Long> copy() {
        MinLongAccumulator result = new MinLongAccumulator();

        result.min = this.min;

        return result;
    }

    @Override
    public void reset() {
        this.min = Long.MAX_VALUE;
    }

    @Override
    public void add(Long in) {
        this.min = Long.min(this.min, in);
    }

    @Override
    public void merge(AccumulatorV2<Long, Long> av) {
        try {
            MinLongAccumulator other = (MinLongAccumulator) av;
            this.min = Long.min(this.min, other.min);
        } catch (ClassCastException ex) {
            throw new UnsupportedOperationException(String.format("Cannot merge %s with %s", this.getClass().getName(), av.getClass().getName()));
        }
    }

    @Override
    public Long value() {
        return this.min;
    }
}

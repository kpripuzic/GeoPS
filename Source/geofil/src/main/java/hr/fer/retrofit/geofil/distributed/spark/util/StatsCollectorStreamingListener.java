/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.distributed.spark.util;

import org.apache.spark.streaming.scheduler.BatchInfo;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import org.apache.spark.streaming.scheduler.StreamingListenerStreamingStarted;
import org.apache.spark.util.LongAccumulator;

/**
 *
 * 
 */
public class StatsCollectorStreamingListener implements StreamingListener {

    private final LongAccumulator numRecords;
    private final LongAccumulator processingDelay;
    private final MinLongAccumulator minTime;
    private final MaxLongAccumulator maxTime;

    public StatsCollectorStreamingListener(LongAccumulator numRecords, LongAccumulator processingDelay, MinLongAccumulator minTime, MaxLongAccumulator maxTime) {
        this.numRecords = numRecords;
        this.processingDelay = processingDelay;
        this.maxTime = maxTime;
        this.minTime = minTime;
    }

    @Override
    public void onStreamingStarted(StreamingListenerStreamingStarted slss) {
    }

    @Override
    public void onReceiverStarted(StreamingListenerReceiverStarted slrs) {
    }

    @Override
    public void onReceiverError(StreamingListenerReceiverError slre) {
    }

    @Override
    public void onReceiverStopped(StreamingListenerReceiverStopped slrs) {
    }

    @Override
    public void onBatchSubmitted(StreamingListenerBatchSubmitted slbs) {
    }

    @Override
    public void onBatchStarted(StreamingListenerBatchStarted slbs) {
        BatchInfo batchInfo = slbs.batchInfo();
        
        if (batchInfo.numRecords() > 0) {
            minTime.add((long) batchInfo.processingStartTime().get());
        }
    }

    @Override
    public void onBatchCompleted(StreamingListenerBatchCompleted slbc) {
        BatchInfo batchInfo = slbc.batchInfo();
        
        long batchNumRecords = batchInfo.numRecords();

        if (batchNumRecords > 0) {
            numRecords.add(batchNumRecords);
            processingDelay.add((long) batchInfo.processingDelay().get());
            maxTime.add((long) batchInfo.processingEndTime().get());
        }
    }

    @Override
    public void onOutputOperationStarted(StreamingListenerOutputOperationStarted sloos) {
    }

    @Override
    public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted slooc) {
    }
}

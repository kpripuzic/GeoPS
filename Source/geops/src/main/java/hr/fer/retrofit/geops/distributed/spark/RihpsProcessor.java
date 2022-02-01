package hr.fer.retrofit.geops.distributed.spark;

import com.carrotsearch.sizeof.RamUsageEstimator;
import hr.fer.retrofit.geops.data.indexing.PartitionedSubscriptionMapFactory;
import hr.fer.retrofit.geops.data.indexing.SpatialIndexFactory;
import hr.fer.retrofit.geops.data.indexing.SpatialIndexFactory.IndexType;
import hr.fer.retrofit.geops.data.model.Publication;
import hr.fer.retrofit.geops.data.model.Subscription;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.util.Utils;
import org.locationtech.jts.index.SpatialIndex;
import scala.Tuple2;

public class RihpsProcessor extends AbstractProcessor {

    public RihpsProcessor(String[] args) {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        final AbstractProcessor processor = new RihpsProcessor(args);

        //check arguments
        String numberOfSubscriptions = processor.cmd.getOptionValue("number-of-subscriptions");
        final String indexType = processor.cmd.getOptionValue("type-of-index");
        final String spatialOperator = processor.cmd.getOptionValue("type-of-spatial-operator", "mixed").equals("mixed") ? ""
                : processor.cmd.getOptionValue("type-of-spatial-operator");
        final int numberOfPartitions = Integer.parseInt(processor.cmd.getOptionValue("number-of-partitions"));

        if (processor.cmd.hasOption("type-of-grid")) {
            System.out.println("Too many parameters for this processor");
            System.exit(1);
        }

        //load and parse subscriptions
        String subscriptionsFile = "subscriptions-" + numberOfSubscriptions + spatialOperator + ".bin";
        List<Subscription> subscriptions = null;
        try ( ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(subscriptionsFile)))) {
            subscriptions = (List<Subscription>) ois.readObject();
        }
        long sizeOfSubscriptions = RamUsageEstimator.sizeOf(subscriptions);
        processor.logger.info("Subscriptions size: " + RamUsageEstimator.humanReadableUnits(sizeOfSubscriptions));

        //add subscriptions to index
        SpatialIndex index = SpatialIndexFactory.createSmall(IndexType.valueOf(indexType), subscriptions);
        long sizeOfIndex = RamUsageEstimator.sizeOf(index);
        processor.logger.info("Index size: " + RamUsageEstimator.humanReadableUnits(sizeOfIndex));

        //partition subscriptions
        List<Tuple2<Integer, Map<Integer, Subscription>>> partitionedSubscriptions = PartitionedSubscriptionMapFactory.createPairs(subscriptions, numberOfPartitions);
        subscriptions = null; //free driver memory
        long sizeOfPartitionedSubscriptions = RamUsageEstimator.sizeOf(partitionedSubscriptions);
        processor.logger.info("Partitioned subscriptions size: " + RamUsageEstimator.humanReadableUnits(sizeOfPartitionedSubscriptions));

        if (!processor.cmd.hasOption("skip-processing")) {

            //broadcast index to all workers
            Broadcast<SpatialIndex> broadcastedIndex = processor.jsc.broadcast(index);
            index = null; //free driver memory

            final Partitioner keyPartitioner = new Partitioner() {
                @Override
                public int numPartitions() {
                    return numberOfPartitions;
                }

                @Override
                public int getPartition(Object o) {
                    return (Integer) o;
                }
            };

            final Partitioner publicationPartitioner = new Partitioner() {
                @Override
                public int numPartitions() {
                    return numberOfPartitions;
                }

                @Override
                public int getPartition(Object o) {
                    return Utils.nonNegativeMod(o.hashCode(), numberOfPartitions);
                }
            };

            //make rdd of partitioned index        
            final JavaPairRDD<Integer, Map<Integer, Subscription>> partitionsRDD = processor.jsc.parallelizePairs(partitionedSubscriptions).partitionBy(keyPartitioner).cache();
            partitionedSubscriptions = null; //free driver memory

            //subscribe to topic and get the stream
            final JavaInputDStream<ConsumerRecord<Void, Object>> stream = KafkaUtils.createDirectStream(processor.jssc, LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<Void, Object>Subscribe(processor.topics, processor.kafkaParams));

            final Function2<Set<Integer>, Set<Integer>, Set<Integer>> mergeCombiners = (set1, set2) -> {
                set1.addAll(set2);
                return set1;
            };

            //brodcast producer props to all workers
            Broadcast<Properties> broadcastedProperties = processor.jsc.broadcast(processor.producerProps);

            //process the stream
            stream.
                    map(record -> (Publication) record.value()).
                    mapToPair(publication -> {
                        List<Integer> candidateSubscriptionIds = broadcastedIndex.value().query(publication.getGeometry().getEnvelopeInternal());
                        return new Tuple2<>(publication, candidateSubscriptionIds);
                    }).
                    mapToPair(pair -> {
                        Map<Integer, Set<Integer>> partitionedCandidateSubscriptionIds = new HashMap<>();

                        for (int candidateSubscriptionId : pair._2) {
                            int partitionId = candidateSubscriptionId % numberOfPartitions;

                            partitionedCandidateSubscriptionIds.compute(partitionId, (key, oldSet) -> {
                                if (oldSet == null) {
                                    Set<Integer> newSet = new HashSet<>();
                                    newSet.add(candidateSubscriptionId);
                                    return newSet;
                                } else {
                                    oldSet.add(candidateSubscriptionId);
                                    return oldSet;
                                }
                            });
                        }

                        return new Tuple2<>(pair._1, partitionedCandidateSubscriptionIds);
                    }).
                    flatMapToPair(pair -> {

                        List<Tuple2<Integer, Tuple2<Publication, Set<Integer>>>> listOfPairs = new LinkedList<>();

                        for (Entry<Integer, Set<Integer>> entry : pair._2.entrySet()) {
                            listOfPairs.add(new Tuple2<>(entry.getKey(), new Tuple2<>(pair._1, entry.getValue())));
                        }

                        return listOfPairs.iterator();
                    }).
                    transformToPair(
                            rdd -> rdd.
                                    partitionBy(keyPartitioner).
                                    join(partitionsRDD).
                                    values().
                                    mapToPair(pair -> {
                                        Set<Subscription> candidateSubscriptions = new HashSet<>();
                                        for (int subscriptionId : pair._1._2) {
                                            candidateSubscriptions.add(pair._2.get(subscriptionId));
                                        }
                                        return new Tuple2<>(pair._1._1, candidateSubscriptions);
                                    })
                    ).
                    //lower pipeline
                    mapToPair(pair -> {
                        Set<Integer> satisfiedSubscriptions = new HashSet<>();

                        for (Subscription candidateSubscription : pair._2) {
                            if (candidateSubscription.getSpatialRelation().apply(candidateSubscription.getGeometry(), pair._1.getGeometry())) {
                                satisfiedSubscriptions.add(candidateSubscription.getId());
                            }
                        }
                        return new Tuple2<>(pair._1, satisfiedSubscriptions);
                    }).
                    filter(pair -> pair._2.size() > 0).
                    reduceByKey(mergeCombiners, publicationPartitioner).
                    foreachRDD(rdd -> {
                        rdd.foreachPartition(iterator -> {
                            Producer<Publication, Set<Integer>> producer = new KafkaProducer<>(broadcastedProperties.value());

                            StreamSupport.
                                    stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.NONNULL), false).
                                    map(pair -> new ProducerRecord<>("geofil-matchings", pair._1, pair._2)).
                                    forEach(record -> producer.send(record));

//                            while (iterator.hasNext()) {
//                                Tuple2<Publication, Set<Integer>> pair = iterator.next();
//                                ProducerRecord<Publication, Set<Integer>> record = new ProducerRecord<>("geofil-matchings", pair._1, pair._2);
//                                producer.send(record);
//                            }
                            producer.close();
                        });
                    });

            //start our streaming context and wait for it to "finish"
            processor.jssc.start();
        }
        //add shutdown hook to collect statistics
        final int finalExperimentNo = Integer.parseInt(processor.cmd.getOptionValue("number-of-experiment"));
        final String finalAppName = getAppName(processor);
        final JavaSparkContext finaljsc = processor.jsc;
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                processor.logger.info("Shutting down streaming app...");

                //print statistics
                final long processingTime = processor.maxTime.value() - processor.minTime.value();
                processor.logger.info(finalAppName);
                processor.logger.info("Number of processed records: " + processor.numRecords.value());
                processor.logger.info("Cumulative processing delay in milliseconds: " + processor.processingDelay.value());
                processor.logger.info("Average processing latency in ms: " + (processor.processingDelay.value() / (double) processor.numRecords.value()));
                processor.logger.info("Processing time in ms: " + processingTime);
                processor.logger.info("Processing throughput in pubs/s: " + 1000 * ((double) processor.numRecords.value()) / processingTime);

                processor.logger.info("Subscriptions size: " + RamUsageEstimator.humanReadableUnits(sizeOfSubscriptions));
                processor.logger.info("Index size: " + RamUsageEstimator.humanReadableUnits(sizeOfIndex));
                processor.logger.info("Partitioned subscriptions size: " + RamUsageEstimator.humanReadableUnits(sizeOfPartitionedSubscriptions));

                //write statistics to hdfs
                try ( PrintWriter pw = new PrintWriter(FileSystem.get(processor.jsc.hadoopConfiguration()).create(new Path("hdfs://"
                        + processor.hdfsHostPort + "/user/kpripuzic/geofil/experiment-" + finalExperimentNo + ".txt")))) {
                    pw.write(finalAppName + "\n");
                    pw.write("Number of processed records: " + processor.numRecords.value() + "\n");
                    pw.write("Cumulative processing delay in milliseconds: " + processor.processingDelay.value() + "\n");
                    pw.write("Average processing latency in ms:" + (processor.processingDelay.value() / (double) processor.numRecords.value()) + "\n");
                    pw.write("Processing time in ms: " + processingTime + "\n");
                    pw.write("Processing throughput in pubs/s: " + 1000 * ((double) processor.numRecords.value() / processingTime) + "\n");

                    pw.write("Subscriptions size: " + RamUsageEstimator.humanReadableUnits(sizeOfSubscriptions) + "\n");
                    pw.write("Index size: " + RamUsageEstimator.humanReadableUnits(sizeOfIndex) + "\n");
                    pw.write("Partitioned subscriptions size: " + RamUsageEstimator.humanReadableUnits(sizeOfPartitionedSubscriptions) + "\n");
                    pw.write("App name and ID: " + finalAppName + " " + finaljsc.sc().applicationId() + "\n");

                    pw.write("Fine print latency: " + finalAppName + " " + (processor.processingDelay.value() / (double) processor.numRecords.value()) + "\n");
                    pw.write("Fine print throughput: " + finalAppName + " " + 1000 * ((double) processor.numRecords.value() / processingTime) + "\n");
                    pw.write("Fine_print_index_size: " + finalAppName + " " + RamUsageEstimator.humanReadableUnits(sizeOfIndex));
                    pw.write("Fine_print_partitioned_subscriptions_size: " + finalAppName + " " + RamUsageEstimator.humanReadableUnits(sizeOfPartitionedSubscriptions));
                } catch (IOException ex) {
                    processor.logger.info("Cannot store statistics to HDFS");
                }

                processor.logger.info("Shutdown of streaming app complete.");
            }
        });

        processor.waitUntilAllConsumed(processor);
    }
}

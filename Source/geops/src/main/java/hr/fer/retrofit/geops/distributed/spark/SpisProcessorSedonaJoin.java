package hr.fer.retrofit.geops.distributed.spark;

import com.carrotsearch.sizeof.RamUsageEstimator;
import hr.fer.retrofit.geops.data.model.Publication;
import hr.fer.retrofit.geops.data.model.Subscription;
import hr.fer.retrofit.geops.data.partitioning.SpatialPartitionerFactory;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.sedona.core.spatialOperator.JoinQuery;
import org.apache.sedona.core.spatialPartitioning.SpatialPartitioner;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.util.Utils;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

public class SpisProcessorSedonaJoin extends AbstractProcessor {

    public SpisProcessorSedonaJoin(String[] args) {
        super(args);
    }

    public static void main(String[] args) throws Exception {
        final AbstractProcessor processor = new SpisProcessorSedonaJoin(args);

        //check arguments
        String numberOfSubscriptions = processor.cmd.getOptionValue("number-of-subscriptions");
        final String spatialOperator = processor.cmd.getOptionValue("type-of-spatial-operator", "mixed").equals("mixed") ? ""
                : processor.cmd.getOptionValue("type-of-spatial-operator");
        final int numberOfPartitions = Integer.parseInt(processor.cmd.getOptionValue("number-of-partitions"));
        final String gridType = processor.cmd.getOptionValue("type-of-grid");
        final String indexType = processor.cmd.getOptionValue("type-of-index");

        //load and parse subscriptions
        String subscriptionsFile = "subscriptions-" + numberOfSubscriptions + spatialOperator + ".bin";
        List<Subscription> subscriptions = null;
        try ( ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(subscriptionsFile)))) {
            subscriptions = (List<Subscription>) ois.readObject();
        }
        List<Geometry> geometrySubscriptions = subscriptions.stream().map(s -> s.getAsGeometryWithUserData()).collect(Collectors.toList());
        subscriptions = null;

        long sizeOfSubscriptions = RamUsageEstimator.sizeOf(geometrySubscriptions);
        processor.logger.info("Subscriptions size: " + RamUsageEstimator.humanReadableUnits(sizeOfSubscriptions));

        //partition subscriptions
        JavaRDD<Geometry> subscriptionRDD = processor.jsc.parallelize(geometrySubscriptions);
        SpatialRDD<Geometry> subscriptionSpatialRDD = new SpatialRDD<>();
        subscriptionSpatialRDD.rawSpatialRDD = subscriptionRDD;
        subscriptionSpatialRDD.analyze();
        subscriptionSpatialRDD.spatialPartitioning(org.apache.sedona.core.enums.GridType.getGridType(gridType),
                SpatialPartitionerFactory.retrieveFakeNumberOfPartitions(gridType, geometrySubscriptions.size(), numberOfPartitions));
        subscriptionSpatialRDD.buildIndex(org.apache.sedona.core.enums.IndexType.getIndexType(indexType), true);
        subscriptionSpatialRDD.indexedRDD.cache();

        //create a partitioner using subscriptions envelopes        
        final SpatialPartitioner partitioner = subscriptionSpatialRDD.getPartitioner();
        processor.logger.info("Number of partitioner partitions: " + partitioner.numPartitions());
        long sizeOfPartitioner = RamUsageEstimator.sizeOf(partitioner);
        processor.logger.info("Partitioner size: " + RamUsageEstimator.humanReadableUnits(sizeOfPartitioner));
        final int exactNumberOfPartitions = partitioner.numPartitions();

        if (!processor.cmd.hasOption("skip-processing")) {
            //subscribe to topic and get the stream
            final JavaInputDStream<ConsumerRecord<Void, Object>> stream = KafkaUtils.createDirectStream(processor.jssc, LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<Void, Object>Subscribe(processor.topics, processor.kafkaParams));

            //brodcast producer props
            final Broadcast<Properties> broadcastedProperties = processor.jsc.broadcast(processor.producerProps);

            final Partitioner publicationPartitioner = new Partitioner() {
                @Override
                public int numPartitions() {
                    return exactNumberOfPartitions;
                }

                @Override
                public int getPartition(Object o) {
                    return Utils.nonNegativeMod(o.hashCode(), exactNumberOfPartitions);
                }
            };

            final Function2<Set<Integer>, Set<Integer>, Set<Integer>> mergeCombiners = (Set<Integer> set1, Set<Integer> set2) -> {
                set1.addAll(set2);
                return set1;
            };

            //process the stream
            stream.
                    transformToPair(publicationRDD -> {
                        //get and partition the publications
                        SpatialRDD<Geometry> publicationsSpatialRDD = new SpatialRDD<>();
                        publicationsSpatialRDD.rawSpatialRDD = publicationRDD.map(record -> ((Publication) record.value()).getAsGeometryWithUserData());
                        publicationsSpatialRDD.analyze();
                        publicationsSpatialRDD.spatialPartitioning(subscriptionSpatialRDD.getPartitioner());

                        //get pairs publication-list of candidate subscriptions
                        JavaPairRDD<Geometry, List<Geometry>> result = JoinQuery.SpatialJoinQuery(subscriptionSpatialRDD, publicationsSpatialRDD, true, true);
                        return result;
                    }).
                    filter(pair -> pair._2.size() > 0).
                    //lower pipeline
                    mapToPair(pair -> {
                        Publication publication = Publication.createFromGeometry(pair._1);
                        Set<Integer> satisfiedSubscriptions = new HashSet<>();
                        for (Geometry geometrySubscription : pair._2) {
                            Subscription candidateSubscription = Subscription.createFromGeometry(geometrySubscription);
                            if (candidateSubscription.getSpatialRelation().apply(candidateSubscription.getGeometry(), publication.getGeometry())) {
                                satisfiedSubscriptions.add(candidateSubscription.getId());
                            }
                        }
                        return new Tuple2<>(publication, satisfiedSubscriptions);
                    }).
                    //reduceByKey(mergeCombiners, publicationPartitioner).
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
//                                //System.out.println(pair._1.getId() + " " + pair._2);
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
        final String finalAppName = getAppName(processor, exactNumberOfPartitions);
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
                processor.logger.info("Partitioner size: " + RamUsageEstimator.humanReadableUnits(sizeOfPartitioner));

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
                    pw.write("Partitioner size: " + RamUsageEstimator.humanReadableUnits(sizeOfPartitioner) + "\n");
                    pw.write("App name and ID: " + finalAppName + " " + finaljsc.sc().applicationId() + "\n");

                    pw.write("Fine print latency: " + finalAppName + " " + (processor.processingDelay.value() / (double) processor.numRecords.value()) + "\n");
                    pw.write("Fine print throughput: " + finalAppName + " " + 1000 * ((double) processor.numRecords.value() / processingTime) + "\n");
                    pw.write("Fine_print_subscription_size: " + finalAppName + " " + RamUsageEstimator.humanReadableUnits(sizeOfSubscriptions) + "\n");
                    pw.write("Fine_print_partitioner_size: " + finalAppName + " " + RamUsageEstimator.humanReadableUnits(sizeOfPartitioner) + "\n");
                } catch (IOException ex) {
                    processor.logger.info("Cannot store statistics to HDFS");
                }

                processor.logger.info("Shutdown of streaming app complete.");
            }
        });

        processor.waitUntilAllConsumed(processor);
    }
}

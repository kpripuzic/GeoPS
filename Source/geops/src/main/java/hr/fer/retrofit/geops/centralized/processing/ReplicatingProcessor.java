package hr.fer.retrofit.geops.centralized.processing;

import com.carrotsearch.sizeof.RamUsageEstimator;
import hr.fer.retrofit.geops.data.indexing.SpatialIndexFactory;
import hr.fer.retrofit.geops.data.indexing.SpatialIndexFactory.IndexType;
import hr.fer.retrofit.geops.data.model.Publication;
import hr.fer.retrofit.geops.data.model.Subscription;
import hr.fer.retrofit.geops.distributed.kafka.serde.ObjectDeserializer;
import hr.fer.retrofit.geops.distributed.kafka.serde.ObjectSerializer;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.locationtech.jts.index.SpatialIndex;

public class ReplicatingProcessor {

    public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException, InterruptedException {

        int experimentNo = 0;
        String numberOfSubscriptions = null;
        String numberOfPublications = null;
        String indexType = null;
        String appName = null;
        String numberOfExecutorThreads = null;

        if (args.length != 5) {
            System.out.println("SparkReplicatingProcessor experiment_number number_of_subscriptions number_of_publications index_type number_of_executor_threads");
            System.exit(1);
        } else {
            experimentNo = Integer.parseInt(args[0]);
            numberOfSubscriptions = args[1];
            numberOfPublications = args[2];
            indexType = args[3];
            numberOfExecutorThreads = args[4];
            appName = String.format("Geofil_cenrepl_exno:%s_nsub:%s_npub:%s_indx:%s_thrds:%s", experimentNo, numberOfSubscriptions, numberOfPublications, indexType, numberOfExecutorThreads);
        }

        System.out.println(appName);

        Properties consumerProps = new Properties();

        consumerProps.put("bootstrap.servers", "broker01:9092,broker02:9092,broker03:9092,broker04:9092");
        consumerProps.put("key.deserializer", IntegerDeserializer.class.getName());
        consumerProps.put("value.deserializer", ObjectDeserializer.class.getName());
        consumerProps.put("group.id", "group-" + experimentNo);
        consumerProps.put("auto.offset.reset", "earliest");
        Collection<String> topics = Arrays.asList("geofil-" + numberOfPublications);

        final LongAccumulator numRecords = new LongAccumulator(Long::sum, 0);
        final LongAccumulator processingDelay = new LongAccumulator(Long::sum, 0);
        final LongAccumulator matchingDelay = new LongAccumulator(Long::sum, 0);
        final LongAccumulator indexDelay = new LongAccumulator(Long::sum, 0);
        final LongAccumulator spopDelay = new LongAccumulator(Long::sum, 0);
        final LongAccumulator spopCount= new LongAccumulator(Long::sum, 0);

        //load and parse subscriptions
        String subscriptionsFile = "subscriptions-" + numberOfSubscriptions + ".bin";
        //load subscriptions from disk
        List<Subscription> subscriptions = null;
        try (ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(subscriptionsFile)))) {
            subscriptions = (List<Subscription>) ois.readObject();
        }

        //load subsriptions from hdfs
//        JavaPairRDD<String, PortableDataStream> binSubscriptions = jsc.binaryFiles("hdfs://" + hdfsHostPort + "/user/kpripuzic/geofil/subscriptions/" + subscriptionsFile, 0);
//        List<Subscription> subscriptions = null;
//        try ( ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(binSubscriptions.first()._2.toArray()))) {
//            subscriptions = (List<Subscription>) ois.readObject();
//        }
        //add subscriptions to index
        SpatialIndex index = SpatialIndexFactory.create(IndexType.valueOf(indexType), subscriptions);

        //get subscriptions size
        System.out.println(appName);
        long sizeOfSubscriptions = RamUsageEstimator.sizeOf(subscriptions);

        System.out.println("Subscriptions size: " + RamUsageEstimator.humanReadableUnits(sizeOfSubscriptions));
        subscriptions = null; //free driver memory
        System.gc();

        //get the index size
        long sizeOfIndex = RamUsageEstimator.sizeOf(index);

        System.out.println("Index size: " + RamUsageEstimator.humanReadableUnits(sizeOfIndex));
        System.gc();

        //brodcast producer props to all workers
        Properties producerProps = new Properties();

        producerProps.put("bootstrap.servers", "broker01:9092,broker02:9092,broker03:9092,broker04:9092");
        producerProps.put("key.serializer", ObjectSerializer.class.getName());
        producerProps.put("value.serializer", ObjectSerializer.class.getName());
        //producerProps.put("value.serializer", LongSerializer.class.getName());

        //below should be paralellized
        Callable<Void> processTask = () -> {

            KafkaConsumer consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(topics);

            Producer<Publication, Set<Integer>> producer = new KafkaProducer<>(producerProps);
            String producerTopic = "geofil-matchings";

            ConsumerRecords<Void, Publication> records = consumer.poll(Duration.ofMillis(100));

            int counter = 0;
            while (counter < 100) {
                for (ConsumerRecord<Void, Publication> consumerRecord : records) {
                    long finalStartTime = System.currentTimeMillis();
                    //map record to pair (publication, list_of_satisfied_subscription_ids)
                    Set<Integer> satisfiedSubscriptions = new HashSet<>();
                    Publication publication = (Publication) consumerRecord.value();

                    long indexStartTime = System.currentTimeMillis();
                    List<Subscription> candidateSubscriptions = index.query(publication.getGeometry().getEnvelopeInternal());
                    indexDelay.accumulate(System.currentTimeMillis() - indexStartTime);
                    for (Subscription candidateSubscription : candidateSubscriptions) {
                        long spopStartTime = System.currentTimeMillis();
                        if (candidateSubscription.getSpatialRelation().apply(candidateSubscription.getGeometry(), publication.getGeometry())) {
                            //notify
                            satisfiedSubscriptions.add(candidateSubscription.getId());
                            //System.out.println(publication.getId() + " " + candidateSubscription.getId() + " " + (System.currentTimeMillis() - spopStartTime));
                            
                        }
                        spopDelay.accumulate(System.currentTimeMillis() - spopStartTime);
                        //System.out.println(publication.getId() + " " + candidateSubscription.getId() + " " + (System.currentTimeMillis() - spopStartTime));
                        spopCount.accumulate(1);
                    }

                    SimpleEntry<Publication, Set<Integer>> pair = new SimpleEntry<>(publication, satisfiedSubscriptions);

                    matchingDelay.accumulate(System.currentTimeMillis() - finalStartTime);

                    //filter pair with empty list_of_satisfied_subscription_ids
                    if (pair.getValue().size() > 0) {
                        //publish each publication for each subscription id in list_of_satisfied_subscription_ids
                        ProducerRecord<Publication, Set<Integer>> producerRecord = new ProducerRecord<>(producerTopic, pair.getKey(), pair.getValue());
                        producer.send(producerRecord);
                        processingDelay.accumulate(System.currentTimeMillis() - finalStartTime);
                        numRecords.accumulate(1);
                    }
                }

                if (records.isEmpty()) {
                    counter++;
                }

                records = consumer.poll(Duration.ofMillis(100));
            }

            producer.close();
            consumer.close();
            return null;
        };

        //above should be parallelized
        int numberOfExecutors = Integer.parseInt(numberOfExecutorThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numberOfExecutors);

        Collection<Callable<Void>> processTasks = new ArrayList<>(numberOfExecutors);
        for (int i = 0; i < numberOfExecutors; i++) {
            processTasks.add(processTask);
        }

        executor.invokeAll(processTasks);
        executor.shutdown();

        //add shutdown hook to collect statistics
        final String finalAppName = appName;

        System.out.println("Shutting down streaming app...");

        //print statistics
        System.out.println(finalAppName);
        System.out.println("Number of processed records: " + numRecords.longValue());
        System.out.println("Cumulative processing delay in milliseconds: " + processingDelay.longValue());
        System.out.println("Average processing time per record: " + ((processingDelay.longValue() / (double) numRecords.longValue()) / (double) numberOfExecutors));
        System.out.println("Cumulative matching delay in milliseconds: " + matchingDelay.longValue());
        System.out.println("Average matching time per record: " + ((matchingDelay.longValue() / (double) numRecords.longValue()) / (double) numberOfExecutors));
        System.out.println("Cumulative index delay in milliseconds: " + indexDelay.longValue());
        System.out.println("Average index time per record: " + ((indexDelay.longValue() / (double) numRecords.longValue()) / (double) numberOfExecutors));
        System.out.println("Cumulative spop delay in milliseconds: " + spopDelay.longValue());
        System.out.println("Average spop time per record: " + ((spopDelay.longValue() / (double) numRecords.longValue()) / (double) numberOfExecutors));
        System.out.println("Number of spops: " + spopCount.longValue());
        System.out.println("Subscriptions size: " + RamUsageEstimator.humanReadableUnits(sizeOfSubscriptions));
        System.out.println("Index size: " + RamUsageEstimator.humanReadableUnits(sizeOfIndex));
        System.out.println("Processing throughput in pubs/s: " + 1000 * ((double) numRecords.longValue()) / (processingDelay.longValue() / (double) numberOfExecutors) + "\n");

        //write statistics to hdfs
        try (PrintWriter pw = new PrintWriter("output" + experimentNo + ".txt")) {
            pw.write(finalAppName + "\n");
            pw.write("Number of processed records: " + numRecords.longValue() + "\n");
            pw.write("Cumulative processing delay in milliseconds: " + processingDelay.longValue() + "\n");
            pw.write("Average processing time per record: " + ((processingDelay.longValue() / (double) numRecords.longValue()) / (double) numberOfExecutors) + "\n");
            pw.write("Cumulative matching delay in milliseconds: " + matchingDelay.longValue() + "\n");
            pw.write("Average matching time per record: " + ((matchingDelay.longValue() / (double) numRecords.longValue()) / (double) numberOfExecutors) + "\n");
            pw.write("Fine print: " + finalAppName + " " + ((processingDelay.longValue() / (double) numRecords.longValue()) / numberOfExecutors) + "\n");
            pw.write("Subscriptions size: " + RamUsageEstimator.humanReadableUnits(sizeOfSubscriptions) + "\n");
            pw.write("Index size: " + RamUsageEstimator.humanReadableUnits(sizeOfIndex) + "\n");
            pw.write("Processing throughput in pubs/s: " + 1000 * ((double) numRecords.longValue()) / (processingDelay.longValue() / (double) numberOfExecutors) + "\n");
        } catch (IOException ex) {
            System.out.println("Cannot store statistics to HDFS");
        }

        System.out.println("Shutdown of centralized app complete.");
    }
}

package hr.fer.retrofit.geofil.data.partitioning;

import hr.fer.retrofit.geofil.data.model.Subscription;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.spark.util.random.SamplingUtils;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.spatialPartitioning.EqualPartitioning;
import org.datasyslab.geospark.spatialPartitioning.FlatGridPartitioner;
import org.datasyslab.geospark.spatialPartitioning.HilbertPartitioning;
import org.datasyslab.geospark.spatialPartitioning.KDBTree;
import org.datasyslab.geospark.spatialPartitioning.KDBTreePartitioner;
import org.datasyslab.geospark.spatialPartitioning.QuadtreePartitioning;
import org.datasyslab.geospark.spatialPartitioning.RtreePartitioning;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialPartitioning.VoronoiPartitioning;
import org.datasyslab.geospark.spatialPartitioning.quadtree.QuadTreePartitioner;
import org.datasyslab.geospark.utils.RDDSampleUtils;
import org.locationtech.jts.geom.Envelope;

public class SpatialPartitionerFactory {

    public static void main(String[] args) throws Exception {
        Map<String, Map<Integer, Map<Integer, Integer>>> fakeMap = new HashMap<>();

        for (GridType gridType : GridType.values()) {
            fakeMap.put(gridType.toString(), new HashMap<>());
            for (int numberOfSubscriptions = 100; numberOfSubscriptions <= 1000000; numberOfSubscriptions *= 10) {
                fakeMap.get(gridType.toString()).put(numberOfSubscriptions, new HashMap<>());

                String niceNumberOfsubscriptions = null;

                switch (numberOfSubscriptions) {
                    case 100:
                        niceNumberOfsubscriptions = "100";
                        break;
                    case 1000:
                        niceNumberOfsubscriptions = "1k";
                        break;
                    case 10000:
                        niceNumberOfsubscriptions = "10k";
                        break;
                    case 100000:
                        niceNumberOfsubscriptions = "100k";
                        break;
                    case 1000000:
                        niceNumberOfsubscriptions = "1M";
                        break;
                }

                String subscriptionsFile = "subscriptions-" + niceNumberOfsubscriptions + ".bin";
                List<Subscription> subscriptions = null;
                try ( ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(subscriptionsFile)))) {
                    subscriptions = (List<Subscription>) ois.readObject();
                }

                for (int requiredNumPartitions = 4; requiredNumPartitions <= 2500; requiredNumPartitions *= 5) {
                    try {
                        int fakeNumberOfPartitions = findFakeNumberOfPartitions(requiredNumPartitions, subscriptions, gridType);
                        fakeMap.get(gridType.toString()).get(numberOfSubscriptions).put(requiredNumPartitions, fakeNumberOfPartitions);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        try ( ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("fakemap.bin")))) {
            oos.writeObject(fakeMap);
        }
    }

    public static int findFakeNumberOfPartitions(int requiredNumPartitions, List<Subscription> subscriptions, GridType gridType) throws Exception {

        if (requiredNumPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be >= 0");
        }

        //take a fraction of first elements from the subscription list
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(requiredNumPartitions, subscriptions.size(), -1);
        final double fraction = SamplingUtils.computeFractionForSampleSize(sampleNumberOfRecords, subscriptions.size(), false);
        int thresholdIndex = Math.round((float) fraction * subscriptions.size());
        List<Envelope> sampledSubscriptionEnvelopes = new LinkedList<>();
        subscriptions.stream().limit(thresholdIndex).forEach(subscription -> sampledSubscriptionEnvelopes.add(subscription.getGeometry().getEnvelopeInternal()));

        //add all subscriptions
        List<Envelope> allSubscriptionEnvelopes = new LinkedList<>();
        for (Subscription subscription : subscriptions) {
            allSubscriptionEnvelopes.add(subscription.getGeometry().getEnvelopeInternal());
        }

        //create a boundary envelope from sampled subscriptions
        Envelope boundaryEnvelope = new Envelope();
        for (Envelope subscriptionEnvelope : allSubscriptionEnvelopes) {
            boundaryEnvelope.expandToInclude(subscriptionEnvelope);
        }

        final Envelope paddedBoundary = new Envelope(
                boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX() + 0.01,
                boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY() + 0.01);

        //int previousNumberOfPartitions = 0;
        for (int fakeNumPartitions = 1; true; fakeNumPartitions++) {
            int numPartitions = 1;

            //previousNumberOfPartitions = numPartitions;
            numPartitions = correctNumberOfPartitions(gridType, paddedBoundary, fakeNumPartitions, sampledSubscriptionEnvelopes);

            if (numPartitions >= requiredNumPartitions) {
                //if (requiredNumPartitions - previousNumberOfPartitions > numPartitions - requiredNumPartitions) {
                System.out.println(gridType + " " + subscriptions.size() + " f:" + fakeNumPartitions + " r:" + requiredNumPartitions + " g:" + numPartitions);
                return fakeNumPartitions;
                //} else {
                //    System.out.println(gridType + " " + subscriptions.size() + " f:" + (fakeNumPartitions - 1) + " r:" + requiredNumPartitions + " g:" + previousNumberOfPartitions);
                //    return fakeNumPartitions - 1;
                //}
            }
        }
    }

    private static int correctNumberOfPartitions(GridType gridType, final Envelope paddedBoundary, int numPartitions, List<Envelope> sampledSubscriptionEnvelopes) throws Exception {
        switch (gridType) {
            case EQUALGRID: {
                EqualPartitioning equalPartitioning = new EqualPartitioning(paddedBoundary, numPartitions);
                SpatialPartitioner partitioner = new FlatGridPartitioner(equalPartitioning.getGrids());
                return partitioner.numPartitions();
            }
            case HILBERT: {
                HilbertPartitioning hilbertPartitioning = new HilbertPartitioning(sampledSubscriptionEnvelopes, paddedBoundary, numPartitions);
                SpatialPartitioner partitioner = new FlatGridPartitioner(hilbertPartitioning.getGrids());
                return partitioner.numPartitions();
            }
            case STRTREE: {
                RtreePartitioning rtreePartitioning = new RtreePartitioning(sampledSubscriptionEnvelopes, numPartitions);
                SpatialPartitioner partitioner = new FlatGridPartitioner(rtreePartitioning.getGrids());
                return partitioner.numPartitions();
            }
            case VORONOI: {
                VoronoiPartitioning voronoiPartitioning = new VoronoiPartitioning(sampledSubscriptionEnvelopes, numPartitions);
                SpatialPartitioner partitioner = new FlatGridPartitioner(voronoiPartitioning.getGrids());
                return partitioner.numPartitions();
            }
            case QUADTREE: {
                QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning(sampledSubscriptionEnvelopes, paddedBoundary, numPartitions);
                SpatialPartitioner partitioner = new QuadTreePartitioner(quadtreePartitioning.getPartitionTree());
                return partitioner.numPartitions();
            }
            case KDBTREE: {
                final KDBTree tree = new KDBTree(sampledSubscriptionEnvelopes.size() / numPartitions, numPartitions, paddedBoundary);
                for (final Envelope sample : sampledSubscriptionEnvelopes) {
                    tree.insert(sample);
                }
                tree.assignLeafIds();
                SpatialPartitioner partitioner = new KDBTreePartitioner(tree);
                return partitioner.numPartitions();
            }
            default:
                return 0;
        }
    }

    public static SpatialPartitioner createExactNumberOfPartitions(GridType gridType, int requiredNumPartitions, Collection<Subscription> subscriptions)
            throws Exception {

        if (requiredNumPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be >= 0");
        }

        int fakeNumPartitions = retrieveFakeNumberOfPartitions(gridType.toString(), subscriptions.size(), requiredNumPartitions);

        //take a fraction of first elements from the subscription list
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(requiredNumPartitions, subscriptions.size(), -1);
        final double fraction = SamplingUtils.computeFractionForSampleSize(sampleNumberOfRecords, subscriptions.size(), false);
        int thresholdIndex = Math.round((float) fraction * subscriptions.size());
        List<Envelope> sampledSubscriptionEnvelopes = new LinkedList<>();
        subscriptions.stream().limit(thresholdIndex).forEach(subscription -> sampledSubscriptionEnvelopes.add(subscription.getGeometry().getEnvelopeInternal()));

        //add all subscriptions
        List<Envelope> allSubscriptionEnvelopes = new LinkedList<>();
        for (Subscription subscription : subscriptions) {
            allSubscriptionEnvelopes.add(subscription.getGeometry().getEnvelopeInternal());
        }

        //create a boundary envelope from sampled subscriptions
        Envelope boundaryEnvelope = new Envelope();
        for (Envelope subscriptionEnvelope : allSubscriptionEnvelopes) {
            boundaryEnvelope.expandToInclude(subscriptionEnvelope);
        }

        final Envelope paddedBoundary = new Envelope(
                boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX() + 0.01,
                boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY() + 0.01);

        switch (gridType) {
            case EQUALGRID: {
                EqualPartitioning EqualPartitioning = new EqualPartitioning(paddedBoundary, fakeNumPartitions);
                return new FlatGridPartitioner(EqualPartitioning.getGrids());
            }
            case HILBERT: {
                HilbertPartitioning hilbertPartitioning = new HilbertPartitioning(sampledSubscriptionEnvelopes, paddedBoundary, fakeNumPartitions);
                return new FlatGridPartitioner(hilbertPartitioning.getGrids());
            }
            case STRTREE: {
                RtreePartitioning rtreePartitioning = new RtreePartitioning(sampledSubscriptionEnvelopes, fakeNumPartitions);
                return new FlatGridPartitioner(rtreePartitioning.getGrids());
            }
            case VORONOI: {
                VoronoiPartitioning voronoiPartitioning = new VoronoiPartitioning(sampledSubscriptionEnvelopes, fakeNumPartitions);
                return new FlatGridPartitioner(voronoiPartitioning.getGrids());
            }
            case QUADTREE: {
                QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning(sampledSubscriptionEnvelopes, paddedBoundary, fakeNumPartitions);
                return new QuadTreePartitioner(quadtreePartitioning.getPartitionTree());
            }
            case KDBTREE: {
                final KDBTree tree = new KDBTree(sampledSubscriptionEnvelopes.size() / fakeNumPartitions, fakeNumPartitions, paddedBoundary);
                for (final Envelope sample : sampledSubscriptionEnvelopes) {
                    tree.insert(sample);
                }
                tree.assignLeafIds();
                return new KDBTreePartitioner(tree);
            }
            default:
                return null;
        }
    }

    public static int retrieveFakeNumberOfPartitions(String gridType, int numberOfSubscriptions, int requiredNumPartitions) throws IllegalArgumentException {
        //get fake number of partitions
        int fakeNumPartitions = 0;
        try ( ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream("fakemap.bin")))) {
            Map<String, Map<Integer, Map<Integer, Integer>>> fakeMap = (Map<String, Map<Integer, Map<Integer, Integer>>>) ois.readObject();
            fakeNumPartitions = fakeMap.get(gridType).get(numberOfSubscriptions).get(requiredNumPartitions);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot find corresponding entry in the fakemap" + e);
        }
        return fakeNumPartitions;
    }

    public static SpatialPartitioner create(GridType gridType, int numPartitions, Collection<Subscription> subscriptions)
            throws Exception {

        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be >= 0");
        }

        //sample a fraction of subscriptions
//        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, subscriptions.size(), -1);
//        final double fraction = SamplingUtils.computeFractionForSampleSize(sampleNumberOfRecords, subscriptions.size(), false);
//        ClassTag<Object> tag = ClassTag$.MODULE$.apply(Subscription.class);
//        BernoulliSampler bs = new BernoulliSampler(fraction, tag);
//        List<Envelope> sampledSubscriptionEnvelopes = new LinkedList<>();
//        scala.collection.Iterator<Subscription> scalaSampledIterator = bs.sample(JavaConversions.asScalaIterator(subscriptions.iterator()));
//        Iterator<Subscription> sampledIterator = JavaConversions.asJavaIterator(scalaSampledIterator);
//        while (sampledIterator.hasNext()) {
//            sampledSubscriptionEnvelopes.add(sampledIterator.next().getGeometry().getEnvelopeInternal());
        //take a fraction of first elements from the subscription list
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, subscriptions.size(), -1);
        final double fraction = SamplingUtils.computeFractionForSampleSize(sampleNumberOfRecords, subscriptions.size(), false);
        int thresholdIndex = Math.round((float) fraction * subscriptions.size());
        List<Envelope> sampledSubscriptionEnvelopes = new LinkedList<>();
        subscriptions.stream().limit(thresholdIndex).forEach(subscription -> sampledSubscriptionEnvelopes.add(subscription.getGeometry().getEnvelopeInternal()));

        //add all subscriptions
        List<Envelope> allSubscriptionEnvelopes = new LinkedList<>();
        for (Subscription subscription : subscriptions) {
            allSubscriptionEnvelopes.add(subscription.getGeometry().getEnvelopeInternal());
        }

        //create a boundary envelope from sampled subscriptions
        Envelope boundaryEnvelope = new Envelope();
        for (Envelope subscriptionEnvelope : allSubscriptionEnvelopes) {
            boundaryEnvelope.expandToInclude(subscriptionEnvelope);
        }

        final Envelope paddedBoundary = new Envelope(
                boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX() + 0.01,
                boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY() + 0.01);

        switch (gridType) {
            case EQUALGRID: {
                EqualPartitioning EqualPartitioning = new EqualPartitioning(paddedBoundary, numPartitions);
                return new FlatGridPartitioner(EqualPartitioning.getGrids());
            }
            case HILBERT: {
                HilbertPartitioning hilbertPartitioning = new HilbertPartitioning(sampledSubscriptionEnvelopes, paddedBoundary, numPartitions);
                return new FlatGridPartitioner(hilbertPartitioning.getGrids());
            }
            case STRTREE: {
                RtreePartitioning rtreePartitioning = new RtreePartitioning(sampledSubscriptionEnvelopes, numPartitions);
                return new FlatGridPartitioner(rtreePartitioning.getGrids());
            }
            case VORONOI: {
                VoronoiPartitioning voronoiPartitioning = new VoronoiPartitioning(sampledSubscriptionEnvelopes, numPartitions);
                return new FlatGridPartitioner(voronoiPartitioning.getGrids());
            }
            case QUADTREE: {
                QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning(sampledSubscriptionEnvelopes, paddedBoundary, numPartitions);
                return new QuadTreePartitioner(quadtreePartitioning.getPartitionTree());
            }
            case KDBTREE: {
                final KDBTree tree = new KDBTree(sampledSubscriptionEnvelopes.size() / numPartitions, numPartitions, paddedBoundary);
                for (final Envelope sample : sampledSubscriptionEnvelopes) {
                    tree.insert(sample);
                }
                tree.assignLeafIds();
                return new KDBTreePartitioner(tree);
            }
            default:
                return null;
        }
    }
}

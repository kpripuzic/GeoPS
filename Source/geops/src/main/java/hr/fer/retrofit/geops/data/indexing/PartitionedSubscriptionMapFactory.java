package hr.fer.retrofit.geops.data.indexing;

import hr.fer.retrofit.geops.data.model.Subscription;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

public class PartitionedSubscriptionMapFactory {

    public static Map<Integer, Map<Integer, Subscription>> create(List<Subscription> subscriptions, SpatialPartitioner partitioner, boolean singlePartition) {
        Map<Integer, Map<Integer, Subscription>> partitionsMap = new HashMap<>();

        for (Subscription subscription : subscriptions) {
            Iterator<Tuple2<Integer, Geometry>> iterator;
            try {
                iterator = partitioner.placeObject(subscription.getGeometry());
                int minPartitionId = partitioner.numPartitions() - 1;
                while (iterator.hasNext()) {
                    int partitionId = iterator.next()._1;

                    if (!singlePartition || partitionId <= minPartitionId) {
                        Map<Integer, Subscription> partitionMap;

                        if ((partitionMap = partitionsMap.get(partitionId)) == null) {
                            partitionMap = new HashMap<>();
                            partitionsMap.put(partitionId, partitionMap);
                        }
                        partitionMap.put(subscription.getId(), subscription);

                        if (partitionId <= minPartitionId) {
                            minPartitionId = partitionId;
                        }
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return partitionsMap;
    }

    public static Map<Integer, Map<Integer, Subscription>> create(List<Subscription> subscriptions, int numberOfPartitions) {
        Map<Integer, Map<Integer, Subscription>> partitionsMap = new HashMap<>();

        for (Subscription subscription : subscriptions) {
            Map<Integer, Subscription> partitionMap;
            int partitionId = subscription.getId() % numberOfPartitions;
            if ((partitionMap = partitionsMap.get(partitionId)) == null) {
                partitionMap = new HashMap<>();
                partitionsMap.put(partitionId, partitionMap);
            }
            partitionMap.put(subscription.getId(), subscription);
        }

        return partitionsMap;
    }

    public static List<Tuple2<Integer, Map<Integer, Subscription>>> createPairs(List<Subscription> subscriptions, SpatialPartitioner partitioner, boolean singlePartition) {
        Map<Integer, Map<Integer, Subscription>> partitionsMap = create(subscriptions, partitioner, singlePartition);

        List<Tuple2<Integer, Map<Integer, Subscription>>> partitionedSubscriptions = new LinkedList();
        for (Map.Entry<Integer, Map<Integer, Subscription>> entry : partitionsMap.entrySet()) {
            partitionedSubscriptions.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }

        return partitionedSubscriptions;
    }

    public static List<Tuple2<Integer, Map<Integer, Subscription>>> createPairs(List<Subscription> subscriptions, int numberOfPartitions) {
        Map<Integer, Map<Integer, Subscription>> partitionsMap = create(subscriptions, numberOfPartitions);

        List<Tuple2<Integer, Map<Integer, Subscription>>> partitionedSubscriptions = new LinkedList();
        for (Map.Entry<Integer, Map<Integer, Subscription>> entry : partitionsMap.entrySet()) {
            partitionedSubscriptions.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }

        return partitionedSubscriptions;
    }
}

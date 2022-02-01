package hr.fer.retrofit.geops.data.indexing;

import hr.fer.retrofit.geops.data.model.Subscription;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;

public class PartitionedSubscriptionsFactory {
   
    public static Map<Integer, Set<Subscription>> create(List<Subscription> subscriptions, SpatialPartitioner partitioner) {
        Map<Integer, Set<Subscription>> partitionMap = new HashMap<>();
        for (Subscription subscription : subscriptions) {
            Iterator<Tuple2<Integer, Geometry>> iterator;
            try {
                iterator = partitioner.placeObject(subscription.getGeometry());
                while (iterator.hasNext()) {
                    Set<Subscription> subscriptionSet;
                    int partitionId = iterator.next()._1;
                    if ((subscriptionSet = partitionMap.get(partitionId)) == null) {
                        subscriptionSet = new HashSet<>();
                        partitionMap.put(partitionId, subscriptionSet);
                    }
                    subscriptionSet.add(subscription);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return partitionMap;
    }

    public static List<Tuple2<Integer, Set<Subscription>>> createPairs(List<Subscription> subscriptions, SpatialPartitioner partitioner) {
        Map<Integer, Set<Subscription>> partitionMap = create(subscriptions, partitioner);

        List<Tuple2<Integer, Set<Subscription>>> partitionedSubscriptions = new LinkedList();
        for (Map.Entry<Integer, Set<Subscription>> entry : partitionMap.entrySet()) {
            partitionedSubscriptions.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }

        return partitionedSubscriptions;
    }
}

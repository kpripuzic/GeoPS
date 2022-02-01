package hr.fer.retrofit.geops.data.indexing;

import hr.fer.retrofit.geops.data.indexing.SpatialIndexFactory.IndexType;
import hr.fer.retrofit.geops.data.model.Subscription;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.hprtree.HPRtree;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

public class PartitionedSpatialIndexFactory {

    public static ArrayList<SpatialIndex> create(IndexType indexType, List<Subscription> subscriptions, SpatialPartitioner partitioner) {
        ArrayList<SpatialIndex> indexes = new ArrayList<>(partitioner.numPartitions());
        for (int i = 0; i < partitioner.numPartitions(); i++) {
            indexes.add(SpatialIndexFactory.create(indexType));
        }

        //add subscriptions to partition indexes
        for (Subscription subscription : subscriptions) {
            Iterator<Tuple2<Integer, Geometry>> iterator;
            try {
                iterator = partitioner.placeObject(subscription.getGeometry());
                iterator.forEachRemaining(pair -> indexes.get(pair._1).insert(subscription.getGeometry().getEnvelopeInternal(), subscription));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        //build spatial index in the case of STRtree ad HPRtree
        for (SpatialIndex index : indexes) {
            if (index instanceof STRtree) {
                ((STRtree) index).build();
            } else if (index instanceof HPRtree) {
                ((HPRtree) index).build();
            }
        }

        return indexes;
    }
    
    public static ArrayList<SpatialIndex> create(IndexType indexType, List<Subscription> subscriptions, int numberOfPartitions) {
        ArrayList<SpatialIndex> indexes = new ArrayList<>(numberOfPartitions);
        for (int i = 0; i < numberOfPartitions; i++) {
            indexes.add(SpatialIndexFactory.create(indexType));
        }

        //add subscriptions to partition indexes
        for (Subscription subscription : subscriptions) {
            int partitionId = subscription.getId() % numberOfPartitions;
            indexes.get(partitionId).insert(subscription.getGeometry().getEnvelopeInternal(), subscription);            
        }

        //build spatial index in the case of STRtree ad HPRtree
        for (SpatialIndex index : indexes) {
            if (index instanceof STRtree) {
                ((STRtree) index).build();
            } else if (index instanceof HPRtree) {
                ((HPRtree) index).build();
            }
        }

        return indexes;
    }

    public static List<Tuple2<Integer, SpatialIndex>> createPairs(IndexType indexType, List<Subscription> subscriptions, SpatialPartitioner partitioner) {
        ArrayList<SpatialIndex> partitionedIndex = create(indexType, subscriptions, partitioner);
        LinkedList<Tuple2<Integer, SpatialIndex>> result = new LinkedList<>();

        for (int i = 0; i < partitionedIndex.size(); i++) {
            result.add(new Tuple2<>(i, partitionedIndex.get(i)));
        }

        return result;
    }
    
    public static List<Tuple2<Integer, SpatialIndex>> createPairs(IndexType indexType, List<Subscription> subscriptions, int numberOfPartitions) {
        ArrayList<SpatialIndex> partitionedIndex = create(indexType, subscriptions, numberOfPartitions);
        LinkedList<Tuple2<Integer, SpatialIndex>> result = new LinkedList<>();

        for (int i = 0; i < partitionedIndex.size(); i++) {
            result.add(new Tuple2<>(i, partitionedIndex.get(i)));
        }

        return result;
    }
}

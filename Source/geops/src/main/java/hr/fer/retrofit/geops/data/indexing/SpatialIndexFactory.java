package hr.fer.retrofit.geops.data.indexing;

import hr.fer.retrofit.geops.data.model.Subscription;
import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.List;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.index.hprtree.HPRtree;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.STRtree;
import scala.Tuple2;

public class SpatialIndexFactory {

    public enum IndexType {
        STR_TREE, QUAD_TREE, HPR_TREE, REGULAR_GRID, MULTILAYER_GRID, LIST_INDEX
    };

    protected static SpatialIndex create(IndexType indexType) {
        switch (indexType) {
            case QUAD_TREE:
                return new Quadtree();
            case STR_TREE:
                return new STRtree();
            case HPR_TREE:
                return new HPRtree();
            case REGULAR_GRID:
                return new RegularGridIndex();
            case MULTILAYER_GRID:
                return new MultilayerGridIndex();
            default:
                return new ListIndex();
        }

    }

    public static SpatialIndex create(IndexType indexType, List<Subscription> subscriptions) {
        SpatialIndex index;
        switch (indexType) {
            case QUAD_TREE:
                index = new Quadtree();
                break;
            case STR_TREE:
                index = new STRtree();
                break;
            case HPR_TREE:
                index = new HPRtree();
                break;
            case REGULAR_GRID:
                index = new RegularGridIndex();
                break;
            case MULTILAYER_GRID:
                index = new MultilayerGridIndex();
                break;
            default:
                index = new ListIndex();
                break;
        }

        //build index before adding subscriptions for RegularGridIndex and MultilayerGridIndex
        if (index instanceof RegularGridIndex | index instanceof MultilayerGridIndex) {
            //create a boundary envelope from subscriptions
            Envelope boundaryEnvelope = new Envelope();
            for (Subscription subscription : subscriptions) {
                boundaryEnvelope.expandToInclude(subscription.getGeometry().getEnvelopeInternal());
            }

            final Envelope paddedBoundary = new Envelope(
                    boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX() + 0.01,
                    boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY() + 0.01);

            if (index instanceof RegularGridIndex) {
                ((RegularGridIndex) index).build(paddedBoundary, 10);
            } else {
                ((MultilayerGridIndex) index).build(paddedBoundary, 1000, 10);
            }
        }
        
        //add subscriptions to index
        subscriptions.forEach(subscription
                -> index.insert(subscription.getGeometry().getEnvelopeInternal(), subscription));

        //build index after adding subscriptions for STRtree and HPRtree
        if (index instanceof STRtree) {
            ((STRtree) index).build();
        } else if (index instanceof HPRtree) {
            ((HPRtree) index).build();
        }

        return index;

    }

    public static SpatialIndex createSmall(IndexType indexType, List<Subscription> subscriptions) {
        SpatialIndex index;
        switch (indexType) {
            case QUAD_TREE:
                index = new Quadtree();
                break;
            case STR_TREE:
                index = new STRtree();
                break;
            case HPR_TREE:
                index = new HPRtree();
                break;
            case REGULAR_GRID:
                index = new RegularGridIndex();
                break;
            case MULTILAYER_GRID:
                index = new MultilayerGridIndex();
                break;
            default:
                index = new ListIndex();
                break;
        }

        //build index before adding subscriptions for RegularGridIndex and MultilayerGridIndex
        if (index instanceof RegularGridIndex | index instanceof MultilayerGridIndex) {
            //create a boundary envelope from subscriptions
            Envelope boundaryEnvelope = new Envelope();
            for (Subscription subscription : subscriptions) {
                boundaryEnvelope.expandToInclude(subscription.getGeometry().getEnvelopeInternal());
            }

            final Envelope paddedBoundary = new Envelope(
                    boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX() + 0.01,
                    boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY() + 0.01);

            if (index instanceof RegularGridIndex) {
                ((RegularGridIndex) index).build(paddedBoundary, 10);
            } else {
                ((MultilayerGridIndex) index).build(paddedBoundary, 1000, 10);
            }
        }

        //add subscriptions to index
        subscriptions.forEach(subscription
                -> index.insert(subscription.getGeometry().getEnvelopeInternal(), subscription.getId()));

        //build index after adding subscriptions for STRtree and HPRtree
        if (index instanceof STRtree) {
            ((STRtree) index).build();
        } else if (index instanceof HPRtree) {
            ((HPRtree) index).build();
        }

        return index;

    }

    public static SpatialIndex createSmallWithPartitions(IndexType indexType, List<Subscription> subscriptions, SpatialPartitioner partitioner) throws Exception {
        SpatialIndex index;
        switch (indexType) {
            case QUAD_TREE:
                index = new Quadtree();
                break;
            case STR_TREE:
                index = new STRtree();
                break;
            case HPR_TREE:
                index = new HPRtree();
                break;
            case REGULAR_GRID:
                index = new RegularGridIndex();
                break;
            case MULTILAYER_GRID:
                index = new MultilayerGridIndex();
                break;
            default:
                index = new ListIndex();
                break;
        }

        //build index before adding subscriptions for RegularGridIndex and MultilayerGridIndex
        if (index instanceof RegularGridIndex | index instanceof MultilayerGridIndex) {
            //create a boundary envelope from subscriptions
            Envelope boundaryEnvelope = new Envelope();
            for (Subscription subscription : subscriptions) {
                boundaryEnvelope.expandToInclude(subscription.getGeometry().getEnvelopeInternal());
            }

            final Envelope paddedBoundary = new Envelope(
                    boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX() + 0.01,
                    boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY() + 0.01);

            if (index instanceof RegularGridIndex) {
                ((RegularGridIndex) index).build(paddedBoundary, 10);
            } else {
                ((MultilayerGridIndex) index).build(paddedBoundary, 1000, 10);
            }
        }

        //add subscriptions to index
        for (Subscription subscription : subscriptions) {
            int minPartitionId = partitioner.numPartitions() - 1;
            Iterator<Tuple2<Integer, Geometry>> iterator = partitioner.placeObject(subscription.getGeometry());
            while (iterator.hasNext()) {
                int partitionId = iterator.next()._1;

                if (partitionId <= minPartitionId) {
                    minPartitionId = partitionId;
                }
            }
            index.insert(subscription.getGeometry().getEnvelopeInternal(), new SimpleEntry<>(minPartitionId, subscription.getId()));
        }

        //build index after adding subscriptions for STRtree and HPRtree
        if (index instanceof STRtree) {
            ((STRtree) index).build();
        } else if (index instanceof HPRtree) {
            ((HPRtree) index).build();
        }

        return index;

    }
}

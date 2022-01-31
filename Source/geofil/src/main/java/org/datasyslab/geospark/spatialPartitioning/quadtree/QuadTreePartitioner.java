/*
 * FILE: QuadTreePartitioner
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geospark.spatialPartitioning.quadtree;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.joinJudgement.DedupParams;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.utils.HalfOpenRectangle;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import scala.Tuple2;

public class QuadTreePartitioner
        extends SpatialPartitioner
{
    private StandardQuadTree<? extends Geometry> quadTree;

    public QuadTreePartitioner(StandardQuadTree<? extends Geometry> quadTree)
    {
        super(GridType.QUADTREE, getLeafGrids(quadTree));
        this.quadTree = quadTree;

        // Make sure not to broadcast all the samples used to build the Quad
        // tree to all nodes which are doing partitioning
        this.quadTree.dropElements();
    }

    @Override
    public <T extends Geometry> Iterator<Tuple2<Integer, T>> placeObject(T spatialObject)
            throws Exception
    {
        Objects.requireNonNull(spatialObject, "spatialObject");

        final Envelope envelope = spatialObject.getEnvelopeInternal();

        final List<QuadRectangle> matchedPartitions = quadTree.findZones(new QuadRectangle(envelope));

        final Point point = spatialObject instanceof Point ? (Point) spatialObject : null;

        final Set<Tuple2<Integer, T>> result = new HashSet<>();
        for (QuadRectangle rectangle : matchedPartitions) {
            // For points, make sure to return only one partition
            if (point != null && !(new HalfOpenRectangle(rectangle.getEnvelope())).contains(point)) {
                continue;
            }

            result.add(new Tuple2(rectangle.partitionId, spatialObject));
        }

        return result.iterator();
    }

    @Nullable
    @Override
    public DedupParams getDedupParams()
    {
        return new DedupParams(grids);
    }

    @Override
    public int numPartitions()
    {
        return grids.size();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof QuadTreePartitioner)) {
            return false;
        }

        final QuadTreePartitioner other = (QuadTreePartitioner) o;
        return other.quadTree.equals(this.quadTree);
    }

    private static List<Envelope> getLeafGrids(StandardQuadTree<? extends Geometry> quadTree)
    {
        Objects.requireNonNull(quadTree, "quadTree");

        final List<QuadRectangle> zones = quadTree.getLeafZones();
        final List<Envelope> grids = new ArrayList<>();
        for (QuadRectangle zone : zones) {
            grids.add(zone.getEnvelope());
        }

        return grids;
    }
}

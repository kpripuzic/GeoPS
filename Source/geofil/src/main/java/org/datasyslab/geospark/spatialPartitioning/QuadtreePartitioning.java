/*
 * FILE: QuadtreePartitioning
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
package org.datasyslab.geospark.spatialPartitioning;

import java.io.Serializable;
import java.util.List;
import org.datasyslab.geospark.spatialPartitioning.quadtree.QuadRectangle;
import org.datasyslab.geospark.spatialPartitioning.quadtree.StandardQuadTree;
import org.locationtech.jts.geom.Envelope;

public class QuadtreePartitioning
        implements Serializable
{

    /**
     * The Quad-Tree.
     */
    private final StandardQuadTree<Integer> partitionTree;

    /**
     * Instantiates a new Quad-Tree partitioning.
     *
     * @param samples the sample list
     * @param boundary the boundary
     * @param partitions the partitions
     */
    public QuadtreePartitioning(List<Envelope> samples, Envelope boundary, int partitions)
            throws Exception
    {
        this(samples, boundary, partitions, -1);
    }

    public QuadtreePartitioning(List<Envelope> samples, Envelope boundary, final int partitions, int minTreeLevel)
            throws Exception
    {
        // Make sure the tree doesn't get too deep in case of data skew
        int maxLevel = partitions;
        int maxItemsPerNode = samples.size() / partitions;
        partitionTree = new StandardQuadTree(new QuadRectangle(boundary), 0,
                maxItemsPerNode, maxLevel);
        if (minTreeLevel > 0) {
            partitionTree.forceGrowUp(minTreeLevel);
        }

        for (final Envelope sample : samples) {
            partitionTree.insert(new QuadRectangle(sample), 1);
        }

        partitionTree.assignPartitionIds();
    }

    public StandardQuadTree getPartitionTree()
    {
        return this.partitionTree;
    }
}

package hr.fer.retrofit.geops.data.indexing;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.index.SpatialIndex;
import scala.Tuple2;

public class MultilayerGridIndex implements SpatialIndex, Serializable {

    private static final long serialVersionUID = -7461163625812743604L;

    protected RegularGridIndex first, second, third;
    protected ListIndex overflow;
    protected double resolutionFactor;

    public MultilayerGridIndex() {
        third = new RegularGridIndex();
        second = new RegularGridIndex();
        first = new RegularGridIndex();
        overflow = new ListIndex();
    }

    public void build(Envelope mbr, int smallestResolution, double resolutionFactor) {
        third.build(mbr, smallestResolution);
        second.build(mbr, (int) (smallestResolution / resolutionFactor));
        first.build(mbr, (int) (smallestResolution / (resolutionFactor * resolutionFactor)));

        this.resolutionFactor = resolutionFactor;
    }

    @Override
    public void insert(Envelope itemEnv, Object item) {

        //try to insert to third
        int col1, col2, row1, row2;
        col1 = (int) Math.floor((itemEnv.getMinX() - third.x) / third.tileWidth);
        col2 = (int) Math.ceil((itemEnv.getMaxX() - third.x) / third.tileWidth);
        row1 = (int) Math.floor((itemEnv.getMinY() - third.y) / third.tileHeight);
        row2 = (int) Math.ceil((itemEnv.getMaxY() - third.y) / third.tileHeight);

        if (col1 < 0) {
            col1 = 0;
        }
        if (row1 < 0) {
            row1 = 0;
        }
        if (col2 >= third.resolution) {
            col2 = third.resolution - 1;
        }
        if (row2 >= third.resolution) {
            row2 = third.resolution - 1;
        }

        if ((row2 - row1) * (col2 - col1) < 4) {
            //add to third
            third.insertToRowsColumns(itemEnv, item, row1, row2, col1, col2);
            return;
        }

        //try to insert to second
        row1 = (int) Math.floor(row1 / resolutionFactor);
        row2 = (int) Math.ceil(row2 / resolutionFactor);
        col1 = (int) Math.floor(col1 / resolutionFactor);
        col2 = (int) Math.ceil(col2 / resolutionFactor);

        if ((row2 - row1) * (col2 - col1) < 4) {
            //add to second
            second.insertToRowsColumns(itemEnv, item, row1, row2, col1, col2);
            return;
        }

        //try to insert to first
        row1 = (int) Math.floor(row1 / resolutionFactor);
        row2 = (int) Math.ceil(row2 / resolutionFactor);
        col1 = (int) Math.floor(col1 / resolutionFactor);
        col2 = (int) Math.ceil(col2 / resolutionFactor);

        if ((row2 - row1) * (col2 - col1) < 10) {
            //add to second
            first.insertToRowsColumns(itemEnv, item, row1, row2, col1, col2);
            return;
        }

        //add to overflow
        overflow.insert(itemEnv, item);
    }

    @Override
    public List query(Envelope searchEnv) {

        //query third
        int col1, col2, row1, row2;
        col1 = (int) Math.floor((searchEnv.getMinX() - third.x) / third.tileWidth);
        col2 = (int) Math.ceil((searchEnv.getMaxX() - third.x) / third.tileWidth);
        row1 = (int) Math.floor((searchEnv.getMinY() - third.y) / third.tileHeight);
        row2 = (int) Math.ceil((searchEnv.getMaxY() - third.y) / third.tileHeight);

        if (col1 < 0) {
            col1 = 0;
        }
        if (row1 < 0) {
            row1 = 0;
        }
        if (col2 >= third.resolution) {
            col2 = third.resolution - 1;
        }
        if (row2 >= third.resolution) {
            row2 = third.resolution - 1;
        }

        Set<Tuple2<Envelope, Object>> pairs = new HashSet<>();
        third.queryRowsColumns(searchEnv, row1, row2, col1, col2, pairs);

        //query second
        row1 = (int) Math.floor(row1 / resolutionFactor);
        row2 = (int) Math.ceil(row2 / resolutionFactor);
        col1 = (int) Math.floor(col1 / resolutionFactor);
        col2 = (int) Math.ceil(col2 / resolutionFactor);
        second.queryRowsColumns(searchEnv, row1, row2, col1, col2, pairs);

        //query first
        row1 = (int) Math.floor(row1 / resolutionFactor);
        row2 = (int) Math.ceil(row2 / resolutionFactor);
        col1 = (int) Math.floor(col1 / resolutionFactor);
        col2 = (int) Math.ceil(col2 / resolutionFactor);
        first.queryRowsColumns(searchEnv, row1, row2, col1, col2, pairs);

        //query overflow
        overflow.query(searchEnv, pairs);

        List result = new LinkedList();
        for (Tuple2<Envelope, Object> pair : pairs) {
            if (pair._1.intersects(searchEnv)) {
                result.add(pair._2);
            }
        }

        return result;
    }

    @Override
    public void query(Envelope searchEnv, ItemVisitor visitor) {
        for (Object object : query(searchEnv)) {
            visitor.visitItem(object);
        }
    }

    @Override
    public boolean remove(Envelope itemEnv, Object item) {

        //try to remove from third
        int col1, col2, row1, row2;
        col1 = (int) Math.floor((itemEnv.getMinX() - third.x) / third.tileWidth);
        col2 = (int) Math.ceil((itemEnv.getMaxX() - third.x) / third.tileWidth);
        row1 = (int) Math.floor((itemEnv.getMinY() - third.y) / third.tileHeight);
        row2 = (int) Math.ceil((itemEnv.getMaxY() - third.y) / third.tileHeight);

        if (col1 < 0) {
            col1 = 0;
        }
        if (row1 < 0) {
            row1 = 0;
        }
        if (col2 >= third.resolution) {
            col2 = third.resolution - 1;
        }
        if (row2 >= third.resolution) {
            row2 = third.resolution - 1;
        }

        if ((row2 - row1) * (col2 - col1) <= 4) {
            //add to third
            return third.removeFromRowsColumns(itemEnv, item, row1, row2, col1, col2);
        }

        //try to remove to second
        row1 = (int) Math.floor(row1 / resolutionFactor);
        row2 = (int) Math.ceil(row2 / resolutionFactor);
        col1 = (int) Math.floor(col1 / resolutionFactor);
        col2 = (int) Math.ceil(col2 / resolutionFactor);

        if ((row2 - row1) * (col2 - col1) <= 4) {
            //add to second
            return second.removeFromRowsColumns(itemEnv, item, row1, row2, col1, col2);
        }

        //try to remove from first
        row1 = (int) Math.floor(row1 / resolutionFactor);
        row2 = (int) Math.ceil(row2 / resolutionFactor);
        col1 = (int) Math.floor(col1 / resolutionFactor);
        col2 = (int) Math.ceil(col2 / resolutionFactor);

        if ((row2 - row1) * (col2 - col1) <= 10) {
            //add to second
            return first.removeFromRowsColumns(itemEnv, item, row1, row2, col1, col2);
        }

        //remove from overflow
        return overflow.remove(itemEnv, item);
    }
}

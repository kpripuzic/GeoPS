package hr.fer.retrofit.geofil.data.indexing;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.index.SpatialIndex;
import scala.Tuple2;

public class RegularGridIndex implements SpatialIndex, Serializable {

    private static final long serialVersionUID = -7461163625812743604L;

    /**
     * Origin of the grid
     */
    protected double x, y;

    /**
     * Width and height of a single tile
     */
    protected double tileWidth, tileHeight;

    /**
     * Total number of columns and rows within the input range
     */
    protected int resolution;

    protected Set<Tuple2<Envelope, Object>>[][] cells;

    public RegularGridIndex() {
    }

    public void build(Envelope mbr, int resolution) {
        this.x = mbr.getMinX();
        this.y = mbr.getMinY();
        this.resolution = resolution;
        this.tileWidth = mbr.getWidth() / resolution;
        this.tileHeight = mbr.getHeight() / resolution;

        //fill cells with empty sets
        this.cells = (Set<Tuple2<Envelope, Object>>[][]) new Set[resolution][resolution];
        for (int i = 0; i < resolution; i++) {
            for (int j = 0; j < resolution; j++) {
                this.cells[i][j] = new HashSet<>();
            }
        }

    }

    @Override
    public void insert(Envelope itemEnv, Object item) {

        int col1, col2, row1, row2;
        col1 = (int) Math.floor((itemEnv.getMinX() - x) / tileWidth);
        col2 = (int) Math.ceil((itemEnv.getMaxX() - x) / tileWidth);
        row1 = (int) Math.floor((itemEnv.getMinY() - y) / tileHeight);
        row2 = (int) Math.ceil((itemEnv.getMaxY() - y) / tileHeight);

        if (col1 < 0) {
            col1 = 0;
        }
        if (row1 < 0) {
            row1 = 0;
        }
        if (col2 >= resolution) {
            col2 = resolution - 1;
        }
        if (row2 >= resolution) {
            row2 = resolution - 1;
        }
        for (int col = col1; col < col2; col++) {
            for (int row = row1; row < row2; row++) {
                cells[row][col].add(new Tuple2<>(itemEnv, item));
            }
        }
    }
    
    void insertToRowsColumns(Envelope itemEnv, Object item, int row1, int row2, int col1, int col2) {
        for (int col = col1; col < col2; col++) {
            for (int row = row1; row < row2; row++) {
                cells[row][col].add(new Tuple2<>(itemEnv, item));
            }
        }
    }
    
    void queryRowsColumns(Envelope searchEnv, int row1, int row2, int col1, int col2, Set<Tuple2<Envelope, Object>> pairs) {
        for (int col = col1; col < col2; col++) {
            for (int row = row1; row < row2; row++) {
                for (Tuple2<Envelope, Object> pair : cells[row][col]) {
                    pairs.add(pair);
                }
            }
        }        
    }

    @Override
    public List query(Envelope searchEnv) {

        int col1, col2, row1, row2;
        col1 = (int) Math.floor((searchEnv.getMinX() - x) / tileWidth);
        col2 = (int) Math.ceil((searchEnv.getMaxX() - x) / tileWidth);
        row1 = (int) Math.floor((searchEnv.getMinY() - y) / tileHeight);
        row2 = (int) Math.ceil((searchEnv.getMaxY() - y) / tileHeight);

        if (col1 < 0) {
            col1 = 0;
        }
        if (row1 < 0) {
            row1 = 0;
        }
        if (col2 >= resolution) {
            col2 = resolution - 1;
        }
        if (row2 >= resolution) {
            row2 = resolution - 1;
        }
        
        Set<Tuple2<Envelope, Object>> pairs = new HashSet<>();
        for (int col = col1; col < col2; col++) {
            for (int row = row1; row < row2; row++) {
                for (Tuple2<Envelope, Object> pair : cells[row][col]) {
                    pairs.add(pair);
                }
            }
        }

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
        int col1, col2, row1, row2;
        col1 = (int) Math.floor((itemEnv.getMinX() - x) / tileWidth);
        col2 = (int) Math.ceil((itemEnv.getMaxX() - x) / tileWidth);
        row1 = (int) Math.floor((itemEnv.getMinY() - y) / tileHeight);
        row2 = (int) Math.ceil((itemEnv.getMaxY() - y) / tileHeight);

        if (col1 < 0) {
            col1 = 0;
        }
        if (row1 < 0) {
            row1 = 0;
        }
        if (col2 >= resolution) {
            col2 = resolution - 1;
        }
        if (row2 >= resolution) {
            row2 = resolution - 1;
        }

        boolean removed = false;

        for (int col = col1; col < col2; col++) {
            for (int row = row1; row < row2; row++) {
                if (cells[row][col].remove(new Tuple2<>(itemEnv, item))) {
                    removed = true;
                }
            }
        }

        return removed;
    }    
    
    public boolean removeFromRowsColumns(Envelope itemEnv, Object item, int row1, int row2, int col1, int col2) {

        boolean removed = false;

        for (int col = col1; col < col2; col++) {
            for (int row = row1; row < row2; row++) {
                if (cells[row][col].remove(new Tuple2<>(itemEnv, item))) {
                    removed = true;
                }
            }
        }

        return removed;
    }    
}

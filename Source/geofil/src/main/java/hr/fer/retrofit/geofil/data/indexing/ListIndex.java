package hr.fer.retrofit.geofil.data.indexing;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.ItemVisitor;
import org.locationtech.jts.index.SpatialIndex;
import scala.Tuple2;

public class ListIndex implements SpatialIndex {

    List<Tuple2<Envelope, Object>> list = new LinkedList<>();

    @Override
    public void insert(Envelope itemEnv, Object item) {
        list.add(new Tuple2<>(itemEnv, item));
    }

    @Override
    public List query(Envelope searchEnv) {
        List result = new LinkedList();
        for (Tuple2<Envelope, Object> pair : list) {
            if (pair._1.intersects(searchEnv)) {
                result.add(pair._2);
            }
        }

        return result;
    }

    public void query(Envelope searchEnv, Set<Tuple2<Envelope, Object>> pairs) {

        for (Tuple2<Envelope, Object> pair : list) {
            if (pair._1.intersects(searchEnv)) {
                pairs.add(pair);
            }
        }
    }

    @Override
    public void query(Envelope searchEnv, ItemVisitor visitor) {
        for (Tuple2<Envelope, Object> pair : list) {
            visitor.visitItem(pair._2);
        }
    }

    @Override
    public boolean remove(Envelope itemEnv, Object item) {
        Iterator<Tuple2<Envelope, Object>> iterator = list.iterator();

        while (iterator.hasNext()) {
            Tuple2<Envelope, Object> pair = iterator.next();
            if (pair._2.equals(item)) {
                iterator.remove();
                return true;
            }
        }

        return false;
    }
}

package hr.fer.retrofit.geops.data.model;

import java.io.Serializable;
import org.locationtech.jts.geom.Geometry;

public abstract class GeospatialObject implements Serializable {

    private int id;
    private Geometry geometry;
   
    public GeospatialObject(Geometry geometry, int id) {
        this.geometry = geometry;
        this.id = id;
    }

    public Geometry getGeometry() {
        return geometry;
    }

    public int getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GeospatialObject) {
            GeospatialObject other = (GeospatialObject) obj;
            if (id == other.id) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "GeospatialObject{" + "id=" + id + ", geometry=" + geometry + '}';
    }
    
    
}

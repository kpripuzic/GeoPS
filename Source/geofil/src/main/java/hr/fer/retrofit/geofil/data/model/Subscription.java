package hr.fer.retrofit.geofil.data.model;

import java.io.Serializable;
import org.locationtech.jts.geom.Geometry;

public class Subscription extends GeospatialObject implements Serializable {
    
    private SpatialRelation spatialRelation;

    public Subscription(Geometry geometry, int id, SpatialRelation spatialRelation) {
        super(geometry, id);
        this.spatialRelation = spatialRelation;
    }

    public SpatialRelation getSpatialRelation() {
        return spatialRelation;
    }
    
    public Geometry getAsGeometryWithUserData() {
        Geometry geometry = super.getGeometry();
        
        String userData = this.getId() + "," + this.spatialRelation;
        geometry.setUserData(userData);
        
        return geometry;
    }
    
    public static Subscription createFromGeometry(Geometry geometry) {
        String userData = (String) geometry.getUserData();
        String splitted[] = userData.split(",");
        int id = Integer.parseInt(splitted[0]);
        SpatialRelation spatialRelation = SpatialRelation.valueOf(splitted[1]);
        geometry.setUserData(null);
               
        return new Subscription(geometry, id, spatialRelation);
    }
}

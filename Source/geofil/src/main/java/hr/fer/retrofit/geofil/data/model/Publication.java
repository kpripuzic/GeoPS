/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hr.fer.retrofit.geofil.data.model;

import java.io.Serializable;
import org.locationtech.jts.geom.Geometry;

/**
 *
 * 
 */
public class Publication extends GeospatialObject implements Serializable {

    public Publication() {
        super(null, 0);
    }

    public Publication(Geometry geometry, int id) {
        super(geometry, id);
    }

    public Geometry getAsGeometryWithUserData() {
        Geometry geometry = super.getGeometry();

        String userData = Integer.toString(this.getId());
        geometry.setUserData(userData);

        return geometry;
    }

    public static Publication createFromGeometry(Geometry geometry) {
        int id = Integer.parseInt((String) geometry.getUserData());
        
        geometry.setUserData(null);

        return new Publication(geometry, id);
    }
}

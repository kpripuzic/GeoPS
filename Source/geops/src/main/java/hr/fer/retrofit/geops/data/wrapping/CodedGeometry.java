package hr.fer.retrofit.geops.data.wrapping;

import org.locationtech.jts.geom.Geometry;

public class CodedGeometry {
    private final Geometry geometry;
    private final String code;

    public CodedGeometry(Geometry geometry, String code) {
        this.geometry = geometry;
        this.code = code;
    }

    public Geometry getGeometry() {
        return geometry;
    }

    public String getCode() {
        return code;
    }        
}

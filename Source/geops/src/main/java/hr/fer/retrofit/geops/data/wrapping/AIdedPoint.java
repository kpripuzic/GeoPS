package hr.fer.retrofit.geops.data.wrapping;

import org.locationtech.jts.geom.Point;

public class AIdedPoint {
    private final Point point;
    private final String ai;
    private final int id;

    public AIdedPoint(Point point, String ai, int id) {
        this.point = point;
        this.ai = ai;
        this.id = id;
    }

    public Point getPoint() {
        return point;
    }

    public String getAI() {
        return ai;
    }   

    public int getId() {
        return id;
    }
}

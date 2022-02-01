package hr.fer.retrofit.geops.data.model;

import java.util.function.BiFunction;
import org.locationtech.jts.geom.Geometry;

public enum SpatialRelation implements BiFunction<Geometry, Geometry, Boolean> {
    COVERED_BY ((g1, g2) -> g1.coveredBy(g2)),
    COVERS ((g1, g2) -> g1.covers(g2)),
    CONTAINS ((g1, g2) -> g1.contains(g2)),
    CROSSES ((g1, g2) -> g1.crosses(g2)),
    EQUALS ((g1, g2) -> g1.equals(g2)),
    INTERSECTS ((g1, g2) -> g1.intersects(g2)),
    OVERLAPS ((g1, g2) -> g1.overlaps(g2)),
    TOUCHES ((g1, g2) -> g1.touches(g2)),
    WITHIN ((g1, g2) -> g1.within(g2)),
    DISJOINT ((g1, g2) -> g1.disjoint(g2));
    
    private final BiFunction<Geometry, Geometry, Boolean> relation;

    private SpatialRelation(BiFunction<Geometry, Geometry, Boolean> relation) {
        this.relation = relation;
    }

    @Override
    public Boolean apply(Geometry t, Geometry u) {
        return relation.apply(t, u);
    }        
}

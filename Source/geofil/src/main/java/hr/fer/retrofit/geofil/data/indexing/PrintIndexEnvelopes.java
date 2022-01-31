package hr.fer.retrofit.geofil.data.indexing;

import hr.fer.retrofit.geofil.data.loading.AccidentDataLoader;
import hr.fer.retrofit.geofil.data.model.SpatialRelation;
import hr.fer.retrofit.geofil.data.model.Subscription;
import hr.fer.retrofit.geofil.data.partitioning.SpatialPartitionerFactory;
import hr.fer.retrofit.geofil.data.wrapping.AIdedPoint;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

public class PrintIndexEnvelopes {

    public static void main(String[] args) throws IOException, Exception {
        int decimals = 10;
        GeometryFactory gf = new GeometryFactory(new PrecisionModel(Math.pow(10, decimals)), 4326);
        String filePath = "dft-accident-data/Accidents0515.csv";
        List<AIdedPoint> codedPoints = AccidentDataLoader.loadCsvFile(filePath, gf);
        Collections.shuffle(codedPoints);

        List<Subscription> subscriptions = new LinkedList<>();

        for (int i = 0; i < codedPoints.size(); i++) {
            AIdedPoint point = ((ArrayList<AIdedPoint>) codedPoints).get(i);
            subscriptions.add(new Subscription(point.getPoint(), i, SpatialRelation.CONTAINS));
        }

        //gf = new GeometryFactory(new PrecisionModel(Math.pow(10, decimals)), 3857);
        SpatialPartitioner partitioner = SpatialPartitionerFactory.create(GridType.VORONOI, 20, subscriptions);
        List<Envelope> envelopes = partitioner.getGrids();

        CoordinateReferenceSystem sourceCRS = org.geotools.referencing.CRS.decode("EPSG:4326", true);
        CoordinateReferenceSystem targetCRS = org.geotools.referencing.CRS.decode("EPSG:3857", true);

        MathTransform transform = org.geotools.referencing.CRS.findMathTransform(sourceCRS, targetCRS);        

        for (Envelope envelope : envelopes) {
            Geometry targetGeometry = JTS.transform(gf.toGeometry(envelope), transform);
            System.out.println(targetGeometry);
        }
    }
}

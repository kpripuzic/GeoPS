package hr.fer.retrofit.geops.data.loading;

import hr.fer.retrofit.geops.data.wrapping.CodedGeometry;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.store.ContentFeatureCollection;
import org.geotools.data.store.ContentFeatureSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.opengis.feature.simple.SimpleFeature;


public class PostcodeDataLoader {
    public static List<CodedGeometry> loadShapeFile(String filePath, GeometryFactory gf) throws MalformedURLException, NoSuchElementException, IOException {
        //sectors
        List<CodedGeometry> result = new LinkedList<>();
        File file = new File(filePath);
        ShapefileDataStore dataStore = new ShapefileDataStore(file.toURI().toURL());
        ContentFeatureSource featureSource = dataStore.getFeatureSource();
        ContentFeatureCollection featureCollection = featureSource.getFeatures();
        try (SimpleFeatureIterator iterator = featureCollection.features()) {
            while (iterator.hasNext()) {
                SimpleFeature feature = iterator.next();
                String code = (String) feature.getAttribute(2);
                GeometryPrecisionReducer gpr = new GeometryPrecisionReducer(gf.getPrecisionModel());
                Geometry originalGeometry = (Geometry) feature.getDefaultGeometry();               
                //reducing the number of decimal digits
                Geometry geometry = (Geometry) gpr.reduce(originalGeometry);              
                //if (code.startsWith("RG")) {
                result.add(new CodedGeometry(geometry, code));
                //}                
            }
        }
        dataStore.dispose();
        return result;
    }
}

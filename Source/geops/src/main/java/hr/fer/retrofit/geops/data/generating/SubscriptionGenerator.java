package hr.fer.retrofit.geops.data.generating;

import hr.fer.retrofit.geops.data.loading.PostcodeDataLoader;
import hr.fer.retrofit.geops.data.model.SpatialRelation;
import hr.fer.retrofit.geops.data.model.Subscription;
import hr.fer.retrofit.geops.data.wrapping.CodedGeometry;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;

public class SubscriptionGenerator {
    //private static final double AREA_PERCENTAGE = 0.1;
    //private static final double DISTRICT_PERCENTAGE = 0.2;
    //SECTOR_PERCENTAGE is 1 - (AREA_PERCENTAGE + DISTRICT_PERCENTAGE)

    //private static final int NUMBER_OF_SUBSCRIPTIONS = 1000;
    //precision of 10 decimal digits
    private static final int DECIMALS = 10;
    private static final GeometryJSON GJ = new GeometryJSON(DECIMALS);
    private static final GeometryFactory GF = new GeometryFactory(new PrecisionModel(Math.pow(10, DECIMALS)), 4326);

    public static void generateAndSaveObjectOutputStream(double areaPercentage, double districtPercentage, int numberOfSubscriptions, String fileName, String spatialRelationString) throws IOException {
        List<Subscription> subscriptions = generate(areaPercentage, districtPercentage, numberOfSubscriptions, spatialRelationString);

        try (ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)))) {
            oos.writeObject(subscriptions);
        }
    }

    public static List<Subscription> generate(double areaPercentage, double districtPercentage, int numberOfSubscriptions, String spatialRelationString) throws IOException {

        //load areas
        String filePath = "GB_Postcodes-QGIS_fixed/PostalArea_fixed.shp";
        List<CodedGeometry> areas = PostcodeDataLoader.loadShapeFile(filePath, GF);
        System.out.println("Areas: " + areas.size());

        //load districts
        filePath = "GB_Postcodes-QGIS_fixed/PostalDistrict_fixed.shp";
        List<CodedGeometry> districts = PostcodeDataLoader.loadShapeFile(filePath, GF);
        System.out.println("Districts: " + districts.size());

        //load sectors
        filePath = "GB_Postcodes-QGIS_fixed/PostalSector_fixed.shp";
        List<CodedGeometry> sectors = PostcodeDataLoader.loadShapeFile(filePath, GF);
        System.out.println("Sectors: " + sectors.size());

        //create result array
        Geometry[] geometries = new Geometry[numberOfSubscriptions];
        int geometryCounter = 0;

        Random random = new Random();
        //generate geometries
        for (int i = 0; i < numberOfSubscriptions; i++) {

            double type = random.nextDouble();
            CodedGeometry geometry;

            if (type < areaPercentage) {
                //generate area subscription
                geometry = areas.get(random.nextInt(areas.size()));
                geometries[geometryCounter++] = geometry.getGeometry();
            } else if (type < areaPercentage + districtPercentage) {
                //generate district subsctiption
                geometry = districts.get(random.nextInt(districts.size()));
                geometries[geometryCounter++] = geometry.getGeometry();
            } else {
                //generate sector subscription
                geometry = sectors.get(random.nextInt(sectors.size()));
                geometries[geometryCounter++] = geometry.getGeometry();
            }
        }

        //sfuffle geometries for subscriptions        
        List<Geometry> shuffledGeometries = Arrays.asList(geometries);
        Collections.shuffle(shuffledGeometries);

        List<Subscription> subscriptions = new ArrayList<>();
        int counter = 0;

        SpatialRelation[] spatialRelations = SpatialRelation.values();
        SpatialRelation spatialRelation;
        for (Geometry geometry : shuffledGeometries) {
            if (spatialRelationString.equals("mixed")) {
                spatialRelation = spatialRelations[random.nextInt(spatialRelations.length - 1)]; //ignore disjoint
            } else {
                spatialRelation = SpatialRelation.valueOf(spatialRelationString);
            }
            subscriptions.add(new Subscription(geometry, counter++, spatialRelation));
        }

        return subscriptions;

        //write shapefile
//        GeometryCollection subscriptions = new GeometryCollection(geometries, new GeometryFactory());
//        FileOutputStream shp = new FileOutputStream("Subscriptions/Subscriptions.shp");
//        FileOutputStream shx = new FileOutputStream("Subscriptions/Subscriptions.shx");
//        try (ShapefileWriter writer = new ShapefileWriter(shp.getChannel(), shx.getChannel())) {
//            writer.write(subscriptions, ShapeType.POLYGON);
//        }
        //write json collection
//        GeometryJSON geom = new GeometryJSON();
//        geom.writeGeometryCollection(subscriptions, "subscriptions.json");
//        //write one json per line
//        GeometryJSON gj = new GeometryJSON(decimals);
//        List<Geometry> shuffledGeometries = Arrays.asList(geometries);
//        Collections.shuffle(shuffledGeometries);
//
//        List<GeoJSONSubscription> savedSubscriptions = new ArrayList<>();
//        SpatialRelation[] relations = SpatialRelation.values();
//        Random random = new Random();
//        int counter = 0;
//        for (Geometry geometry : shuffledGeometries) {
//            savedSubscriptions.add(new GeoJSONSubscription(new Subscription(GeospatialObjectType.SUBSCRIPTION, 
//                    geometry, counter++, relations[random.nextInt(relations.length)])));
//        }
//
//        try (FileWriter fw = new FileWriter("subscriptions.json")) {
//            for (GeoJSONSubscription subscription : savedSubscriptions) {
//                fw.append(subscription.toGeoJson(gj) + "\n");
//            }
//        }
//
//        //load json subscriptions from disk
//        Stream<String> lines = Files.lines(Path.of("subscriptions.json"));
//        List<GeoJSONSubscription> loadedSubscriptions = new ArrayList<>(savedSubscriptions.size());
//
//        //parse and add subscriptions to list
//        lines.map(unchecked(line -> GeoJSONSubscription.fromGeoJSON(line, gj))).forEach(subscription
//                -> loadedSubscriptions.add(subscription));
//        System.out.println("Number of loaded subscriptions: " + loadedSubscriptions.size());
//
//        counter = 0;
//        for (int i = 0; i < numberOfSubscriptions; i++) {
//            if (!loadedSubscriptions.get(i).equals(savedSubscriptions.get(i))) {
//                counter++;
//            }
//        }
//
//        System.out.println("Number of unequal subscriptions: " + counter);
    }
   
    @FunctionalInterface
    public interface ThrowingFunction<T, R, E extends Throwable> {

        R apply(T t) throws E;

        static <T, R, E extends Throwable> Function<T, R> unchecked(ThrowingFunction<T, R, E> f) {
            return t -> {
                try {
                    return f.apply(t);
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            };
        }
    }

    public static void main(String[] args) throws IOException {
        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 100, "subscriptions-100.bin", "mixed");
        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 1000, "subscriptions-1k.bin", "mixed");
        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 10000, "subscriptions-10k.bin", "mixed");
        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 100000, "subscriptions-100k.bin", "mixed");
        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 1000000, "subscriptions-1M.bin", "mixed");
        
//        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 10000, "subscriptions-10k" + SpatialRelation.CONTAINS.toString() + ".bin", SpatialRelation.CONTAINS.toString());
//        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 10000, "subscriptions-10k" + SpatialRelation.COVERED_BY.toString() + ".bin", SpatialRelation.COVERED_BY.toString());
//        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 10000, "subscriptions-10k" + SpatialRelation.COVERS.toString() + ".bin", SpatialRelation.COVERS.toString());
//        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 10000, "subscriptions-10k" + SpatialRelation.CROSSES.toString() + ".bin", SpatialRelation.CROSSES.toString());
//        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 10000, "subscriptions-10k" + SpatialRelation.EQUALS.toString() + ".bin", SpatialRelation.EQUALS.toString());
//        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 10000, "subscriptions-10k" + SpatialRelation.INTERSECTS.toString() + ".bin", SpatialRelation.INTERSECTS.toString());
//        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 10000, "subscriptions-10k" + SpatialRelation.OVERLAPS.toString() + ".bin", SpatialRelation.OVERLAPS.toString());
//        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 10000, "subscriptions-10k" + SpatialRelation.TOUCHES.toString() + ".bin", SpatialRelation.TOUCHES.toString());
//        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 10000, "subscriptions-10k" + SpatialRelation.WITHIN.toString() + ".bin", SpatialRelation.WITHIN.toString());
//        SubscriptionGenerator.generateAndSaveObjectOutputStream(0.2, 0.3, 10000, "subscriptions-10k" + SpatialRelation.DISJOINT.toString() + ".bin", SpatialRelation.DISJOINT.toString());
//        
    }
}

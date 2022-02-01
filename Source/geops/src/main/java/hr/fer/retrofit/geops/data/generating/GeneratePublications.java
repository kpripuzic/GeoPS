package hr.fer.retrofit.geops.data.generating;

import hr.fer.retrofit.geops.data.loading.AccidentDataLoader;
import hr.fer.retrofit.geops.data.loading.PostcodeDataLoader;
import hr.fer.retrofit.geops.data.model.Publication;
import hr.fer.retrofit.geops.data.wrapping.AIdedPoint;
import hr.fer.retrofit.geops.data.wrapping.CodedGeometry;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.index.strtree.STRtree;

public class GeneratePublications {

    private static final double AREA_PERCENTAGE = 0.1;
    private static final double DISTRICT_PERCENTAGE = 0.2;
    private static final double SECTOR_PERCENTAGE = 0.3;
    //POINT_PERCENTAGE is 1 - (AREA_PERCENTAGE + DISTRICT_PERCENTAGE + SECTOR_PERCENTAGE)

    private static final int NUMBER_OF_PUBLICATIONS = 10010;

    public static void main(String[] args) throws IOException {
        //presision of 10 decimal digits
        int decimals = 10;
        GeometryFactory gf = new GeometryFactory(new PrecisionModel(Math.pow(10, decimals)), 4326);

        //load areas
        String filePath = "GB_Postcodes-QGIS_fixed/PostalArea_fixed.shp";
        List<CodedGeometry> areas = PostcodeDataLoader.loadShapeFile(filePath, gf);
        System.out.println("Areas: " + areas.size());

        STRtree areaIndex = new STRtree();
        for (CodedGeometry area : areas) {
            areaIndex.insert(area.getGeometry().getEnvelopeInternal(), area);
        }
        areaIndex.build();

        //load districts
        filePath = "GB_Postcodes-QGIS_fixed/PostalDistrict_fixed.shp";
        List<CodedGeometry> districts = PostcodeDataLoader.loadShapeFile(filePath, gf);
        System.out.println("Districts: " + districts.size());

        STRtree districtIndex = new STRtree();
        for (CodedGeometry district : districts) {
            districtIndex.insert(district.getGeometry().getEnvelopeInternal(), district);
        }
        districtIndex.build();

        //load sectors
        filePath = "GB_Postcodes-QGIS_fixed/PostalSector_fixed.shp";
        List<CodedGeometry> sectors = PostcodeDataLoader.loadShapeFile(filePath, gf);
        System.out.println("Sectors: " + sectors.size());

        STRtree sectorIndex = new STRtree();
        for (CodedGeometry sector : sectors) {
            sectorIndex.insert(sector.getGeometry().getEnvelopeInternal(), sector);
        }
        sectorIndex.build();

        //load points
        filePath = "dft-accident-data/Accidents0515.csv";
        List<AIdedPoint> codedPoints = AccidentDataLoader.loadCsvFile(filePath, gf);
        System.out.println("Points: " + codedPoints.size());

        //generate publications
        LinkedList<Geometry> geometries = codedPoints.parallelStream().limit(NUMBER_OF_PUBLICATIONS).map(point -> {
            List<CodedGeometry> matchingAreas = areaIndex.query(point.getPoint().getEnvelopeInternal());
            List<CodedGeometry> matchingDistricts = districtIndex.query(point.getPoint().getEnvelopeInternal());
            List<CodedGeometry> matchingSectors = sectorIndex.query(point.getPoint().getEnvelopeInternal());

            int conditionCounter = 0;
            for (CodedGeometry matchingArea : matchingAreas) {
                if (matchingArea.getGeometry().covers(point.getPoint())) {
                    conditionCounter++;
                    break;
                }
            }
            for (CodedGeometry matchingDistrict : matchingDistricts) {
                if (matchingDistrict.getGeometry().covers(point.getPoint())) {
                    conditionCounter++;
                    break;
                }
            }
            for (CodedGeometry matchingSector : matchingSectors) {
                if (matchingSector.getGeometry().covers(point.getPoint())) {
                    conditionCounter++;
                    break;
                }
            }
            if (conditionCounter == 3) {
                //point is valid
                double type = ThreadLocalRandom.current().nextDouble();

                if (type < AREA_PERCENTAGE) {
                    //generate area publication
                    for (CodedGeometry matchingArea : matchingAreas) {
                        if (matchingArea.getGeometry().covers(point.getPoint())) {
                            return matchingArea.getGeometry();
                        }
                    }

                } else if (type < AREA_PERCENTAGE + DISTRICT_PERCENTAGE) {
                    //generate district publication
                    for (CodedGeometry matchingDistrict : matchingDistricts) {
                        if (matchingDistrict.getGeometry().covers(point.getPoint())) {
                            return matchingDistrict.getGeometry();
                        }
                    }
                } else if (type < AREA_PERCENTAGE + DISTRICT_PERCENTAGE + SECTOR_PERCENTAGE) {
                    //generate sector publication
                    for (CodedGeometry matchingSector : matchingSectors) {
                        if (matchingSector.getGeometry().covers(point.getPoint())) {
                            return matchingSector.getGeometry();
                        }
                    }
                } else {
                    //generate point publication
                    return point.getPoint();
                }
            }
            return null;
        }).filter(geometry -> geometry != null).collect(LinkedList<Geometry>::new, (ll, geometry) -> ll.add(geometry), (ll1, ll2) -> {
            ll1.addAll(ll2);
        });

        System.out.println("Valid publications: " + geometries.size());

        //randomize the order i.e. shuffle publications       
        GeometryJSON gj = new GeometryJSON(decimals);
        List<Geometry> shuffledGeometries = new LinkedList<>(geometries);
        Collections.shuffle(shuffledGeometries);

        List<Publication> savedPublications = new ArrayList<>();
        int counter = 0;
        for (Geometry geometry : shuffledGeometries) {
            savedPublications.add(new Publication(geometry, counter++));
        }

        //save jsonable publications 
//        try ( FileWriter fw = new FileWriter("publications.json")) {
//            for (Publication publication : savedPublications) {
//                fw.append(new JsonablePublication(publication).toGeoJson(gj) + "\n");
//            }
//        }
        //load jsonable publications and count equal to the saved
//        final Iterator<Publication> iterator = savedPublications.iterator();
//        long equals = Files.lines(Path.of("publications.json")).
//                map(unchecked(line -> JsonablePublication.fromGeoJSON(line, gj).getPublication())).
//                filter(publication -> publication.equals(iterator.next())).count();
//
//        System.out.println("Equal publications: " + equals);
//        Total time:  07:13 h
        //save serialized publications
        try ( ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("publications.bin")))) {
            for (Publication publication : savedPublications) {
                oos.writeObject(publication);
            }
        }

        //load serialized publications and count equal to the saved
        int equals = 0;
        try (ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream("publications.bin")))) {
            for (Publication publication : savedPublications) {
                try {
                    Publication readPublication = (Publication) ois.readObject();
                    if (readPublication.equals(publication)) {
                        equals++;
                    }
                } catch (ClassNotFoundException ex) {
                    //will not happen
                }
            }
        }
               

        System.out.println("Equal publications: " + equals);
        //Total time:  05:42 h
    }
}

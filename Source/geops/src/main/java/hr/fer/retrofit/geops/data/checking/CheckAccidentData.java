package hr.fer.retrofit.geops.data.checking;

import hr.fer.retrofit.geops.data.loading.AccidentDataLoader;
import static hr.fer.retrofit.geops.data.loading.PostcodeDataLoader.loadShapeFile;
import hr.fer.retrofit.geops.data.wrapping.AIdedPoint;
import hr.fer.retrofit.geops.data.wrapping.CodedGeometry;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.index.strtree.STRtree;

public class CheckAccidentData {

    public static void main(String[] args) throws IOException {
        //presision of 10 decimal digits
        int decimals = 10;
        GeometryFactory gf = new GeometryFactory(new PrecisionModel(Math.pow(10, decimals)), 4326);

        //load areas
        String filePath = "GB_Postcodes-QGIS_fixed/PostalArea_fixed.shp";
        List<CodedGeometry> areas = loadShapeFile(filePath, gf);

        STRtree areaIndex = new STRtree();
        for (CodedGeometry area : areas) {
            areaIndex.insert(area.getGeometry().getEnvelopeInternal(), area);
        }
        areaIndex.build();

        //load districts
        filePath = "GB_Postcodes-QGIS_fixed/PostalDistrict_fixed.shp";
        List<CodedGeometry> districts = loadShapeFile(filePath, gf);

        STRtree districtIndex = new STRtree();
        for (CodedGeometry district : districts) {
            districtIndex.insert(district.getGeometry().getEnvelopeInternal(), district);
        }
        districtIndex.build();

        //load sectors
        filePath = "GB_Postcodes-QGIS_fixed/PostalSector_fixed.shp";
        List<CodedGeometry> sectors = loadShapeFile(filePath, gf);
        System.out.println("Sectors: " + sectors.size());

        //count sectors covered by districts
        STRtree sectorIndex = new STRtree();
        for (CodedGeometry sector : sectors) {
            sectorIndex.insert(sector.getGeometry().getEnvelopeInternal(), sector);
        }
        sectorIndex.build();

        //load points
        filePath = "dft-accident-data/Accidents0515.csv";
        List<AIdedPoint> codedPoints = AccidentDataLoader.loadCsvFile(filePath, gf);
        System.out.println("Points: " + codedPoints.size());

        ConcurrentLinkedQueue<Point> pointQueue = new ConcurrentLinkedQueue<>();
        long time = System.currentTimeMillis();
        codedPoints.stream().parallel().forEach(point -> {
            List<CodedGeometry> matchingAreas = areaIndex.query(point.getPoint().getEnvelopeInternal());
            List<CodedGeometry> matchingDistricts = districtIndex.query(point.getPoint().getEnvelopeInternal());
            List<CodedGeometry> matchingSectors = sectorIndex.query(point.getPoint().getEnvelopeInternal());

            int counter = 0;
            for (CodedGeometry matchingArea : matchingAreas) {
                if (matchingArea.getGeometry().covers(point.getPoint())) {
                    counter++;
                    break;
                }
            }
            for (CodedGeometry matchingDistrict : matchingDistricts) {
                if (matchingDistrict.getGeometry().covers(point.getPoint())) {
                    counter++;
                    break;
                }
            }
            for (CodedGeometry matchingSector : matchingSectors) {
                if (matchingSector.getGeometry().covers(point.getPoint())) {
                    counter++;
                    break;
                }
            }
            if (counter == 3) {
                pointQueue.add(point.getPoint());
            }
        });
        System.out.println("Covered points: " + pointQueue.size());
        System.out.println("Elapsed time: " + ((System.currentTimeMillis() - time)) / 60000d + " minutes");
    }
}

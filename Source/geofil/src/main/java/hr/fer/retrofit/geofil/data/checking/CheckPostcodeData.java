package hr.fer.retrofit.geofil.data.checking;

import static hr.fer.retrofit.geofil.data.loading.PostcodeDataLoader.loadShapeFile;
import hr.fer.retrofit.geofil.data.wrapping.CodedGeometry;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.index.strtree.STRtree;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * 
 */
public class CheckPostcodeData {

    public static void main(String[] args) throws MalformedURLException, IOException {
        //presision of 10 decimal digits
        int decimals = 10;
        GeometryFactory gf = new GeometryFactory(new PrecisionModel(Math.pow(10, decimals)), 4326);

        //load areas
        String filePath = "GB_Postcodes-QGIS_fixed/PostalArea_fixed.shp";
        List<CodedGeometry> areas = loadShapeFile(filePath, gf);
        System.out.println("Areas: " + areas.size());

        //load districts
        filePath = "GB_Postcodes-QGIS_fixed/PostalDistrict_fixed.shp";
        List<CodedGeometry> districts = loadShapeFile(filePath, gf);
        System.out.println("Districts: " + districts.size());

        //count districts covered by area
        STRtree districtIndex = new STRtree();
        for (CodedGeometry district : districts) {
            districtIndex.insert(district.getGeometry().getEnvelopeInternal(), district);
        }
        districtIndex.build();

        Set<CodedGeometry> districtSet = ConcurrentHashMap.newKeySet();
        areas.stream().parallel().forEach(area -> {
            //System.out.print(area.getCode() + ": ");
            List<CodedGeometry> matchingDistricts = districtIndex.query(area.getGeometry().getEnvelopeInternal());
            for (CodedGeometry matchingDistrict : matchingDistricts) {
                if (area.getGeometry().covers(matchingDistrict.getGeometry())) {
                    districtSet.add(matchingDistrict);
                }
            }
            //System.out.println("");
        });
        System.out.println("Covered districts: " + districtSet.size());

        //find non-covered districts
        STRtree areaIndex = new STRtree();
        for (CodedGeometry area : areas) {
            areaIndex.insert(area.getGeometry().getEnvelopeInternal(), area);
        }
        areaIndex.build();
        
        districts.stream().parallel().forEach(district -> {
            List<CodedGeometry> matchingAreas = areaIndex.query(district.getGeometry().getEnvelopeInternal());
            boolean flag = false;

            for (CodedGeometry matchingArea : matchingAreas) {
                if (matchingArea.getGeometry().covers(district.getGeometry())) {
                    flag = true;
                } else {
                    matchingArea.getGeometry().covers(district.getGeometry());
                }
            }

            if (!flag) {
                System.out.println(district.getCode());
            }
        });
        
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

        Set<CodedGeometry> sectorSet = ConcurrentHashMap.newKeySet();
        districts.stream().parallel().forEach(district -> {
            //System.out.print(district.getCode() + ": ");
            List<CodedGeometry> matchingSectors = sectorIndex.query(district.getGeometry().getEnvelopeInternal());
            for (CodedGeometry matchingSector : matchingSectors) {
                if (district.getGeometry().covers(matchingSector.getGeometry())) {
                    //System.out.print(matchingSector.getCode() + ", ");
                    sectorSet.add(matchingSector);
                }
            }
            //System.out.println("");
        });
        System.out.println("Covered sectors: " + sectorSet.size());

        //find non-covered sectors
        sectors.stream().parallel().forEach(sector -> {
            List<CodedGeometry> matchingDistricts = districtIndex.query(sector.getGeometry().getEnvelopeInternal());
            boolean flag = false;

            for (CodedGeometry matchingDistrict : matchingDistricts) {
                if (matchingDistrict.getGeometry().covers(sector.getGeometry())) {
                    flag = true;
                } else {
                    matchingDistrict.getGeometry().covers(sector.getGeometry());
                }
            }

            if (!flag) {
                System.out.println(sector.getCode());
            }
        });
    }    
}

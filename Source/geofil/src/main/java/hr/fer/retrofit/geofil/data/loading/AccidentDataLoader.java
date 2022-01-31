package hr.fer.retrofit.geofil.data.loading;

import hr.fer.retrofit.geofil.data.wrapping.AIdedPoint;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

public class AccidentDataLoader {

    public static ArrayList<AIdedPoint> loadCsvFile(String filePath, GeometryFactory gf) throws FileNotFoundException, IOException {
        ArrayList<AIdedPoint> result = null;
        AtomicInteger count = new AtomicInteger(0);
        try ( Stream<String> stream = Files.lines(Paths.get(filePath))) {
            // skip first line
            result = stream.map(line -> line.split(",")).
                    filter(splittedLine -> isSpittedLineValid(splittedLine)).
                    map(splittedLine -> new AIdedPoint(gf.createPoint(new Coordinate(Double.parseDouble(splittedLine[3]),
                    Double.parseDouble(splittedLine[4]))), splittedLine[0], count.getAndIncrement())).
                    collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        }

        return result;
    }

    private static boolean isSpittedLineValid(String[] splittedLine) {
        try {
            Double.parseDouble(splittedLine[3]);
            Double.parseDouble(splittedLine[4]);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }
}

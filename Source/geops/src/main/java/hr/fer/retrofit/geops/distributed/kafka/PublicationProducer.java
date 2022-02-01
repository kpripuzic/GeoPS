package hr.fer.retrofit.geops.distributed.kafka;

import hr.fer.retrofit.geops.data.loading.AccidentDataLoader;
import hr.fer.retrofit.geops.data.loading.PostcodeDataLoader;
import hr.fer.retrofit.geops.data.model.Publication;
import hr.fer.retrofit.geops.data.wrapping.AIdedPoint;
import hr.fer.retrofit.geops.data.wrapping.CodedGeometry;
import hr.fer.retrofit.geops.distributed.kafka.serde.ObjectSerializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.index.strtree.STRtree;

public class PublicationProducer {

    private static final double AREA_PERCENTAGE = 0.1;
    private static final double DISTRICT_PERCENTAGE = 0.2;
    private static final double SECTOR_PERCENTAGE = 0.3;
    private static final double POLYGON_PERCENTAGE = AREA_PERCENTAGE + DISTRICT_PERCENTAGE + SECTOR_PERCENTAGE;
    //POINT_PERCENTAGE is 1 - (AREA_PERCENTAGE + DISTRICT_PERCENTAGE + SECTOR_PERCENTAGE)

    public static void main(String[] args) throws IOException, InterruptedException {
        //check arguments
        int numberOfPublications = 0;

        String topic = "";
        if (args.length != 3) {
            System.out.println("PublicationProducer topic_name number_of_publications point_percentage");
            System.exit(1);
        } else {
            final double pointPercentage, areaPercentage, districtPercentage, sectorPercentage;
            numberOfPublications = Integer.parseInt(args[1]);
            topic = args[0];
            pointPercentage = Double.parseDouble(args[2]);
            areaPercentage = (1 - pointPercentage) * AREA_PERCENTAGE / POLYGON_PERCENTAGE;
            districtPercentage = (1 - pointPercentage) * DISTRICT_PERCENTAGE / POLYGON_PERCENTAGE;
            sectorPercentage = (1 - pointPercentage) * SECTOR_PERCENTAGE / POLYGON_PERCENTAGE;

            //load data
            System.out.println("Loading...");

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
            ArrayList<AIdedPoint> codedPoints = AccidentDataLoader.loadCsvFile(filePath, gf);
            System.out.println("Points: " + codedPoints.size());

            //generate publications
            System.out.println("Generating...");

            //parallelize
            int numberOfProcessors = Runtime.getRuntime().availableProcessors();
            ExecutorService executor = Executors.newFixedThreadPool(numberOfProcessors - 1);
            final int numberOfPublicationsPerTask = (int) (1.5 * Math.ceil(numberOfPublications / (numberOfProcessors - 1d)));

            Callable<List<Publication>> generateTask = () -> {
                List<Publication> publicationsPerTask = new LinkedList<>();

                while (publicationsPerTask.size() < numberOfPublicationsPerTask) {
                    //randomly generate a point
                    int pointId = ThreadLocalRandom.current().nextInt(codedPoints.size());

                    AIdedPoint point = codedPoints.get(pointId);

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

                        if (type < areaPercentage) {
                            //generate area publication
                            for (CodedGeometry matchingArea : matchingAreas) {
                                if (matchingArea.getGeometry().covers(point.getPoint())) {
                                    publicationsPerTask.add(new Publication(matchingArea.getGeometry(), pointId));
                                }
                            }

                        } else if (type < areaPercentage + districtPercentage) {
                            //generate district publication
                            for (CodedGeometry matchingDistrict : matchingDistricts) {
                                if (matchingDistrict.getGeometry().covers(point.getPoint())) {
                                    publicationsPerTask.add(new Publication(matchingDistrict.getGeometry(), pointId));
                                }
                            }
                        } else if (type < areaPercentage + districtPercentage + sectorPercentage) {
                            //generate sector publication
                            for (CodedGeometry matchingSector : matchingSectors) {
                                if (matchingSector.getGeometry().covers(point.getPoint())) {
                                    publicationsPerTask.add(new Publication(matchingSector.getGeometry(), pointId));
                                }
                            }
                        } else {
                            //generate point publication
                            publicationsPerTask.add(new Publication(point.getPoint(), pointId));
                        }
                    }
                }

                return publicationsPerTask;

            };

            Collection<Callable<List<Publication>>> generateTasks = new ArrayList<>(numberOfProcessors - 1);
            for (int i = 0; i < numberOfProcessors - 1; i++) {
                generateTasks.add(generateTask);
            }

            List<List<Publication>> publicationsList = executor.invokeAll(generateTasks)
                    .stream()
                    .map(future -> {
                        try {
                            return future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new IllegalStateException(e);
                        }
                    })
                    .collect(Collectors.toList());

            //producing
            System.out.println("Producing...");

            Properties producerProps = new Properties();
            final String bootstrapServers = "broker01:9092,broker02:9092,broker03:9092,broker04:9092";
            producerProps.put("bootstrap.servers", bootstrapServers);
            producerProps.put("key.serializer", IntegerSerializer.class.getName());
            producerProps.put("value.serializer", ObjectSerializer.class.getName());
            producerProps.put("acks", "all");

            //non-parallel production
            final LongAdder counter = new LongAdder();
            Collection<Callable<Void>> produceTasks = new ArrayList<>(numberOfProcessors - 1);
            for (int i = 0; i < numberOfProcessors - 1; i++) {
                //produceTasks.add(new ProduceTask(producerProps, publicationsList.get(i), topic, counter));
                try {
                    new ProduceTask(producerProps, publicationsList.get(i), topic, counter, numberOfPublications).call();
                } catch (Exception ex) {
                    //do nothing
                }
            }
            executor.invokeAll(produceTasks);
            executor.shutdown();
            System.out.println("Finished...");
        }
    }

    private static class ProduceTask implements Callable<Void> {

        private final Properties producerProps;
        private final Iterator<Publication> publicationsPerTaskIterator;
        private final String topic;
        private final LongAdder counter;
        private final int numberOfPublications;

        public ProduceTask(Properties producerProps, List<Publication> publicationsPerTask, String topic, LongAdder counter, int numberOfPublictions) {
            this.producerProps = producerProps;
            this.publicationsPerTaskIterator = publicationsPerTask.iterator();
            this.topic = topic;
            this.counter = counter;
            this.numberOfPublications = numberOfPublictions;
        }

        @Override
        public Void call() {
            Producer<Void, Publication> producer = new KafkaProducer<>(producerProps);
            while (counter.longValue() < numberOfPublications) {
                Publication publication = publicationsPerTaskIterator.next();
                ProducerRecord<Void, Publication> producerRecord = new ProducerRecord<>(topic, publication);

                try {
                    producer.send(producerRecord).get(2, TimeUnit.SECONDS);
                    counter.increment();
                    System.out.println("Publication " + counter.longValue() + " produced");
                } catch (Exception ex) {
                    //do nothing
                }
            }
            producer.close();
            return null;
        }
    }
}

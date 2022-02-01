package hr.fer.retrofit.geops.distributed.spark;

import hr.fer.retrofit.geops.distributed.kafka.serde.ObjectDeserializer;
import hr.fer.retrofit.geops.distributed.kafka.serde.ObjectSerializer;
import hr.fer.retrofit.geops.distributed.spark.util.MaxLongAccumulator;
import hr.fer.retrofit.geops.distributed.spark.util.MinLongAccumulator;
import hr.fer.retrofit.geops.distributed.spark.util.StatsCollectorStreamingListener;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;

public abstract class AbstractProcessor {

    protected CommandLine cmd;
    protected final Map<String, Object> kafkaParams;
    protected final Collection<String> topics;
    protected final Logger logger;
    protected final String hdfsHostPort;

    protected final JavaStreamingContext jssc;
    protected final SparkConf sparkConf;
    protected final JavaSparkContext jsc;

    protected final LongAccumulator numRecords;
    protected final LongAccumulator processingDelay;
    protected final MaxLongAccumulator maxTime;
    protected final MinLongAccumulator minTime;

    protected final Properties producerProps;
    
    protected final int numberOfRecordsToProcess;

    public AbstractProcessor(String[] args) {
        Options options = parseArgs(args);
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("command", options);
            System.exit(1);
        }

        logger = Logger.getLogger(this.getClass());
        setLoggerLevel();

        logger.info(getAppName(this));

        sparkConf = new SparkConf().setAppName(getAppName(this));
        initializeSparkConf();
        hdfsHostPort = sparkConf.get("spark.master").contains("local") ? "primary:8020" : "";

        kafkaParams = new HashMap<>();
        initializeKafkaParams();
        topics = Arrays.asList("geofil-" + cmd.getOptionValue("number-of-publications"));

        //initialize spark context
        jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        jsc = jssc.sparkContext();
        numRecords = jsc.sc().longAccumulator();
        processingDelay = jsc.sc().longAccumulator();
        maxTime = new MaxLongAccumulator();
        jsc.sc().register(maxTime);
        minTime = new MinLongAccumulator();
        jsc.sc().register(minTime);
        jssc.addStreamingListener(new StatsCollectorStreamingListener(numRecords, processingDelay, minTime, maxTime));

        producerProps = new Properties();
        producerProps.put("bootstrap.servers", "broker01:9092,broker02:9092,broker03:9092,broker04:9092");
        producerProps.put("key.serializer", ObjectSerializer.class.getName());
        producerProps.put("value.serializer", ObjectSerializer.class.getName());
        
        if (!cmd.hasOption("number-of-records-to-process")) {
            System.out.println("Number of records to process has to be defined");
            System.exit(1);
        }
        numberOfRecordsToProcess = Integer.parseInt(cmd.getOptionValue("number-of-records-to-process"));
    }

    //this is required for slow strategies
    protected void waitUntilAllConsumed(AbstractProcessor processor) {
        try {

//            long prevNumRecords = 0;
//
//            do {
//                prevNumRecords = processor.numRecords.value();
//                Thread.sleep(Duration.ofMinutes(10).getSeconds() * 1000);
//            } while (prevNumRecords < processor.numRecords.value());
            
            //processed records - 9859
            do {                
                Thread.sleep(Duration.ofMinutes(1).getSeconds() * 1000);
            } while (processor.numRecords.value() < numberOfRecordsToProcess);
            

            processor.logger.info("Kafka stream empty");            
            processor.jssc.stop(true, true);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    private void initializeSparkConf() {
        switch (cmd.getOptionValue("type-of-executors")) {
            case "tiny":
                logger.info("tiny executors...");
                sparkConf.set("spark.executor.cores", "1");
                sparkConf.set("spark.executor.instances", "96");
                sparkConf.set("spark.executor.memory", "8G");
                break;
            case "small":
                logger.info("small executors...");
                sparkConf.set("spark.executor.cores", "2");
                sparkConf.set("spark.executor.instances", "48");
                sparkConf.set("spark.executor.memory", "16G");
                break;
            case "balanced":
                logger.info("balanced 32 executors...");
                sparkConf.set("spark.executor.cores", "3");
                sparkConf.set("spark.executor.instances", "32");
                sparkConf.set("spark.executor.memory", "24G");
                break;
            case "balanced16":
                logger.info("balanced 16 executors...");
                sparkConf.set("spark.executor.cores", "3");
                sparkConf.set("spark.executor.instances", "16");
                sparkConf.set("spark.executor.memory", "24G");
                break;
            case "balanced8":
                logger.info("balanced 8 executors...");
                sparkConf.set("spark.executor.cores", "3");
                sparkConf.set("spark.executor.instances", "8");
                sparkConf.set("spark.executor.memory", "24G");
                break;
            case "balanced4":
                logger.info("balanced 4 executors...");
                sparkConf.set("spark.executor.cores", "3");
                sparkConf.set("spark.executor.instances", "4");
                sparkConf.set("spark.executor.memory", "24G");
                break;
            case "balanced2":
                logger.info("balanced 2 executors...");
                sparkConf.set("spark.executor.cores", "3");
                sparkConf.set("spark.executor.instances", "2");
                sparkConf.set("spark.executor.memory", "24G");
                break;
            case "balanced1":
                logger.info("balanced 1 executors...");
                sparkConf.set("spark.executor.cores", "3");
                sparkConf.set("spark.executor.instances", "1");
                sparkConf.set("spark.executor.memory", "24G");
                break;
            case "fat":
                logger.info("fat executors...");
                sparkConf.set("spark.executor.cores", "6");
                sparkConf.set("spark.executor.instances", "16");
                sparkConf.set("spark.executor.memory", "48G");
                break;
            case "fat8":
                logger.info("fat 8 executors...");
                sparkConf.set("spark.executor.cores", "6");
                sparkConf.set("spark.executor.instances", "8");
                sparkConf.set("spark.executor.memory", "48G");
                break;
            case "fat4":
                logger.info("fat 4 executors...");
                sparkConf.set("spark.executor.cores", "6");
                sparkConf.set("spark.executor.instances", "4");
                sparkConf.set("spark.executor.memory", "48G");
                break;
            case "fat2":
                logger.info("fat 2 executors...");
                sparkConf.set("spark.executor.cores", "6");
                sparkConf.set("spark.executor.instances", "2");
                sparkConf.set("spark.executor.memory", "48G");
                break;
            case "fat1":
                logger.info("fat 1 executors...");
                sparkConf.set("spark.executor.cores", "6");
                sparkConf.set("spark.executor.instances", "1");
                sparkConf.set("spark.executor.memory", "48G");
                break;
            case "oversized":
                logger.info("oversized executors...");
                sparkConf.set("spark.executor.cores", "8");
                sparkConf.set("spark.executor.instances", "16");
                sparkConf.set("spark.executor.memory", "48G");
                break;
//            case "oversized-cro-ngi":
//                logger.info("fat cro-ngi executors...");
//                sparkConf.set("spark.executor.cores", "16");
//                sparkConf.set("spark.executor.instances", "9");
//                sparkConf.set("spark.executor.memory", "16G");
//                break;
        }

        sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        sparkConf.set("spark.rpc.message.maxSize", "1024");
        if (cmd.hasOption("number-of-concurrent-jobs")) {
            sparkConf.set("spark.streaming.concurrentJobs", cmd.getOptionValue("number-of-concurrent-jobs"));
        }
        if (cmd.hasOption("max-rate-per-partition")) {
            sparkConf.set("spark.streaming.kafka.maxRatePerPartition", cmd.getOptionValue("max-rate-per-partition"));
        }

        //set the master if not already set through the command line
        try {
            sparkConf.get("spark.master");
        } catch (NoSuchElementException ex) {
            sparkConf.setMaster("local[*]");
        }
    }

    private void setLoggerLevel() {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
    }

    protected static final String getAppName(AbstractProcessor processor) {
        StringBuilder sb = new StringBuilder(processor.getClass().getSimpleName());
        sb.append("-");
        for (Option option : processor.cmd.getOptions()) {
            if (option.hasArg()) {
                sb.append(option.getOpt());
                sb.append(":");
                sb.append(processor.cmd.getOptionValue(option.getLongOpt()));
                sb.append(",");
            } else {
                sb.append(option.getOpt());
                sb.append(":");
                sb.append(processor.cmd.hasOption(option.getOpt()));
                sb.append(",");
            }
        }
        return removeLastChar(sb.toString());
    }

    protected static final String getAppName(AbstractProcessor processor, int exactNumberOfPartitions) {
        StringBuilder sb = new StringBuilder(processor.getClass().getSimpleName());
        sb.append("-");
        for (Option option : processor.cmd.getOptions()) {
            if (option.hasArg()) {
                sb.append(option.getOpt());
                sb.append(":");
                if (option.getLongOpt().equals("number-of-partitions")) {
                    sb.append(exactNumberOfPartitions);
                } else {
                    sb.append(processor.cmd.getOptionValue(option.getLongOpt()));
                }
                sb.append(",");
            } else {
                sb.append(option.getOpt());
                sb.append(":");
                sb.append(processor.cmd.hasOption(option.getOpt()));
                sb.append(",");
            }
        }
        return removeLastChar(sb.toString());
    }

    public static String removeLastChar(String str) {
        return removeLastChars(str, 1);
    }

    public static String removeLastChars(String str, int chars) {
        return str.substring(0, str.length() - chars);
    }

    private void initializeKafkaParams() {
        kafkaParams.put("bootstrap.servers", "broker01:9092,broker02:9092,broker03:9092,broker04:9092");
        kafkaParams.put("key.deserializer", IntegerDeserializer.class.getName());
        kafkaParams.put("value.deserializer", ObjectDeserializer.class.getName());
        kafkaParams.put("group.id", "group-" + cmd.getOptionValue("number-of-experiment"));
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put("max.poll.interval.ms", "1800000"); //set to 30 minutes instead of default 5 minutes since Sedona is to slow
    }

    private Options parseArgs(String[] args) {
        Options options = new Options();

        Option option = new Option("nex", "number-of-experiment", true, "the number of this experiment");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("nsb", "number-of-subscriptions", true, "the number of subscriptions in this experiment");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("npb", "number-of-publications", true, "the number of publications in this experiment");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("tex", "type-of-executors", true, "the type of executors used in this experiment");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("tix", "type-of-index", true, "the type of index used in this experiment");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("tgd", "type-of-grid", true, "the type of grid used in this experiment");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("npa", "number-of-partitions", true, "the number of subscription partitions in this experiment");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("tso", "type-of-spatial-operator", true, "the type of spatial operator used in this experiment");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("spc", "skip-processing", false, "skip the processing of data stream");
        option.setRequired(false);
        options.addOption(option);

        option = new Option("ncj", "number-of-concurrent-jobs", true, "the number of cuncurrent jobs");
        option.setRequired(true);
        options.addOption(option);
        
        option = new Option("nrp", "number-of-records-to-process", true, "the number of records to process");
        option.setRequired(true);
        options.addOption(option);
        
        option = new Option("mrp", "max-rate-per-partition", true, "max rate per Kafka partition");
        option.setRequired(true);
        options.addOption(option);

        return options;
    }
}

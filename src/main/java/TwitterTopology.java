import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class TwitterTopology {

    static final String CONSUMER_KEY = "dm4bAx8mW8pfmLSQ0pShIC1py";
    static final String CONSUMER_SECRET_KEY = "WyPjXNT9hbJXws9uNizNv5azXiuWMIbb7rhWJg1hfcC0mK2iw2";
    static final String ACCESS_TOKEN = "1231046815622909952-GAcHAorrlqeydGesGvu9ds53V0TflT";
    static final String ACCESS_SECRET_TOKEN = "5bS4ZFG81pUOEeMZJ7AYoRT81JqskOSWZFr9jMMu3Mq6l";
    static final int NUM_WORKERS = 4;
    static final String PARALLEL_ARGUMENT = "parallel";
    static final String NON_PARALLEL_ARGUMENT = "non-parallel";

    public static void main(String[] args) throws Exception {

        double epsilon =  0.002f;
        double threshold = -1;
        int numberOfWorkers = 0;
        String outputPath = "/s/chopin/k/grad/dkielman/twitter/HashTags";

        if(args.length <= 0 || args.length > 5) {
            System.out.println("ERROR: Usage is jar <parallel/non-parallel> <epsilon> [<threshold: default is -1> <output-path: default is twitter/HashTags> <number-of-workers: default is 1>]");
            System.exit(0);
        }

        String topologyType = args[0];

        if (args.length > 1) {
            epsilon = Double.parseDouble(args[1]);
            if (args.length > 2) {
                threshold = Double.parseDouble(args[2]);
                if (args.length > 3) {
                    outputPath = args[3];
                    if (args.length > 4) {
                        numberOfWorkers = Integer.parseInt(args[4]);
                    }
                }
            }
        }

        Config conf = new Config();
        TopologyBuilder builder = new TopologyBuilder();

        // building for parallel topology
        if (topologyType.equalsIgnoreCase(PARALLEL_ARGUMENT)) {
            System.out.println("Running Topology of type: " + topologyType);

            conf.setDebug(true);
            //conf.setNumWorkers(NUM_WORKERS);
            conf.setNumWorkers(numberOfWorkers);
            builder.setSpout("twitter-spout", new TwitterSpout(CONSUMER_KEY,
                    CONSUMER_SECRET_KEY, ACCESS_TOKEN, ACCESS_SECRET_TOKEN));

           // builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt(), NUM_WORKERS)
                    //.shuffleGrouping("twitter-spout");

            builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt(), numberOfWorkers)
                    .shuffleGrouping("twitter-spout");

            //builder.setBolt("lossy-counting-bolt", new LossyParallelCountingBolt(epsilon, threshold), NUM_WORKERS)
                    //.globalGrouping("twitter-hashtag-reader-bolt");

            builder.setBolt("lossy-counting-bolt", new LossyParallelCountingBolt(epsilon, threshold), numberOfWorkers)
                    .globalGrouping("twitter-hashtag-reader-bolt");

            builder.setBolt("aggregate-bolt", new AggregateBolt())
                    .globalGrouping("lossy-counting-bolt");

            builder.setBolt("log-file-bolt", new LogFileBolt(outputPath))
                    .globalGrouping("aggregate-bolt");

            StormSubmitter.submitTopology("TwitterHashtagStormParallel", conf,
                    builder.createTopology());
        } else if (topologyType.equalsIgnoreCase(NON_PARALLEL_ARGUMENT)) {
            System.out.println("Running Topology of type: " + topologyType);
            // building for non-parallel topology
            builder.setSpout("twitter-spout", new TwitterSpout(CONSUMER_KEY,
                    CONSUMER_SECRET_KEY, ACCESS_TOKEN, ACCESS_SECRET_TOKEN));

            builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
                    .shuffleGrouping("twitter-spout");

            builder.setBolt("lossy-counting-bolt", new LossyCountingBolt(epsilon, threshold))
                    .globalGrouping("twitter-hashtag-reader-bolt");

            builder.setBolt("log-file-bolt", new LogFileBolt(outputPath))
                    .globalGrouping("lossy-counting-bolt");
            conf.setDebug(true);

            StormSubmitter.submitTopology("TwitterHashtagStorm", conf,
                    builder.createTopology());
        }
    }
}
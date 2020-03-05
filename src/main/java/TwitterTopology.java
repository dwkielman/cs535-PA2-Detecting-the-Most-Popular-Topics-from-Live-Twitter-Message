import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;

public class TwitterTopology extends ConfigurableTopology{

    static final String CONSUMER_KEY = "dm4bAx8mW8pfmLSQ0pShIC1py";
    static final String CONSUMER_SECRET_KEY = "WyPjXNT9hbJXws9uNizNv5azXiuWMIbb7rhWJg1hfcC0mK2iw2";
    static final String ACCESS_TOKEN = "1231046815622909952-GAcHAorrlqeydGesGvu9ds53V0TflT";
    static final String ACCESS_SECRET_TOKEN = "5bS4ZFG81pUOEeMZJ7AYoRT81JqskOSWZFr9jMMu3Mq6l";

    public static void main(String[] args) {
        ConfigurableTopology.start(new TwitterTopology(), args);
    }

    protected int run(String args[]) throws Exception {

        double testingEpsilon =  0.002f;
        //double testingThreshold = 0.3;
        double testingThreshold = -1;
        String testingOutputPath = "/s/chopin/k/grad/dkielman/twitter/HashTags.txt";

        // currently building for non-parallel topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("twitter-spout", new TwitterSpout(CONSUMER_KEY,
                CONSUMER_SECRET_KEY, ACCESS_TOKEN, ACCESS_SECRET_TOKEN));

        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
                .shuffleGrouping("twitter-spout");

        builder.setBolt("lossy-counting-bolt", new LossyCountingBolt(testingEpsilon, testingThreshold))
                .globalGrouping("twitter-hashtag-reader-bolt");

        builder.setBolt("log-file-bolt", new LogFileBolt(testingOutputPath))
                .globalGrouping("lossy-counting-bolt");
        conf.setDebug(true);

        String topologyName = "TwitterHashtagStorm";

        return submit(topologyName, conf, builder);

    }
}

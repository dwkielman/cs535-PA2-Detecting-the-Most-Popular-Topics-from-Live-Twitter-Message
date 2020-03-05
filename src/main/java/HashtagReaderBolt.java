import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.*;

public class HashtagReaderBolt implements IRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        //System.out.println("HashtagReaderBolt completed initialization");
    }

    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField("tweet");
        for(HashtagEntity hashtage : tweet.getHashtagEntities()) {
            String ht = hashtage.getText().toLowerCase().trim();
            //System.out.println("HashtagReaderBolt sending Hashtag: " + ht);
            if (!ht.isEmpty()) {
                this.collector.emit(tuple, new Values(ht));
                this.collector.ack(tuple);
            }
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hashtag"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
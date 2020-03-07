
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class AggregateBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private static final long TIME_INTERVAL = 10000;
    private ConcurrentHashMap<Integer, HashMap<String, Integer>> topTags = new ConcurrentHashMap<Integer, HashMap<String, Integer>>();
    private static long currentStartTime;
    private static final String recordDelimiter = "##";
    private static final String valueDelimiter = ":";

    public AggregateBolt() { }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        currentStartTime = System.currentTimeMillis();
    }

    @Override
    public void execute(Tuple tuple) {
        String tags = tuple.getStringByField("tags");
        HashMap<String, Integer> receivedTags = formatTags(tags);
        topTags.put(tuple.getSourceTask(), receivedTags);

        // at each 10 seconds, need to submit to the Log
        long currentTime = System.currentTimeMillis();

        if (currentTime >= currentStartTime + TIME_INTERVAL) {
            if (!topTags.isEmpty()) {
                HashMap<String, Integer> topHundredMap = combineAndSortTopTags(topTags.values());

                if (!topHundredMap.isEmpty()) {
                    List<String> output = new LinkedList<String>();
                    for (String s : topHundredMap.keySet()) {
                        output.add("<" + s + " frequency: " + topHundredMap.get(s) + ">");
                    }
                    outputCollector.emit(tuple, new Values(output.toString(), currentTime));
                    outputCollector.ack(tuple);
                }
            }
            currentStartTime = currentTime;
        }
    }

    private synchronized HashMap<String, Integer> combineAndSortTopTags(Collection<HashMap<String, Integer>> tagsList) {
        HashMap<String, Integer> combinedTagsMap = new HashMap<String, Integer>();

        for (HashMap<String, Integer> maps : tagsList) {
            for (String s : maps.keySet()) {
                if (!combinedTagsMap.containsKey(s)) {
                    combinedTagsMap.put(s, maps.get(s));
                } else {
                    int tempFreq = combinedTagsMap.get(s);
                    tempFreq = tempFreq + maps.get(s);
                    combinedTagsMap.put(s, tempFreq);
                }
            }
        }

        HashMap<String, Integer> topHundred =
                combinedTagsMap.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .limit(100)
                        .collect(Collectors.toMap(
                                Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        return topHundred;
    }

    private HashMap<String, Integer> formatTags(String tags) {
        HashMap<String, Integer> returnMap = new HashMap<String, Integer>();
        List<String> flatTags = Arrays.asList(tags.split(recordDelimiter));
        for (String s : flatTags) {
            String[] split = s.split(valueDelimiter);
            if (split.length == 2) {
                returnMap.put(split[0], Integer.parseInt(split[1]));
            }
        }
        return returnMap;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tags", "time"));
    }

}

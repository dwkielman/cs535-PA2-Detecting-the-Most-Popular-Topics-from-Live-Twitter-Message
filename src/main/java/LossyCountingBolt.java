import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LossyCountingBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private static final long TIME_INTERVAL = 10000;
    private long bucketWidth;
    private final AtomicInteger bucketNumber = new AtomicInteger(1);
    private final AtomicInteger numElements = new AtomicInteger(0);
    private ConcurrentHashMap<String, BucketObject> bucket = new ConcurrentHashMap<String, BucketObject>();
    private static long currentStartTime;
    private double threshold;
    private double epsilon;

    public LossyCountingBolt(double epsilon, double s) {
        this.epsilon = epsilon;
        this.threshold = s;
        this.bucketWidth = (long) Math.ceil(1 / this.epsilon);
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        currentStartTime = System.currentTimeMillis();
        //System.out.println("LossyCountingBolt completed initialization with start time: " + currentStartTime);
    }

    @Override
    public void execute(Tuple tuple) {
        numElements.incrementAndGet();

        // get the current hashtag
        String hashtag = tuple.getStringByField("hashtag");

        // add the hashtag if we aren't storing it already
        if (!bucket.containsKey(hashtag)) {
            bucket.put(hashtag, new BucketObject(hashtag, 1, (bucketNumber.get() - 1)));
            //System.out.println("LossyCountingBolt received new Hashtag: " + hashtag);
        } else {
            BucketObject bucketObject = bucket.get(hashtag);
            bucketObject.incrementFrequency();
            bucket.put(hashtag, bucketObject);
            System.out.println("LossyCountingBolt bucket is currently of size: " + bucket.size());
        }

        // at each 10 seconds, need to submit the most recent bucket to the Log
        long currentTime = System.currentTimeMillis();

        if (currentTime >= currentStartTime + TIME_INTERVAL) {
            //System.out.println("Writing the data to file after 10 seconds");
            if (!bucket.isEmpty()) {
                List<BucketObject> entries = emitBucket(bucket);
                if (!entries.isEmpty()) {
                    List<String> output = new LinkedList<String>();
                    for (BucketObject bo : entries) {
                        output.add("<" + bo.getHashtag() + " frequency: " + bo.getFrequency() + ">");
                    }
                    //System.out.println("output.size() = " + output.size());
                    outputCollector.emit(tuple, new Values(output.toString(), currentTime));
                    outputCollector.ack(tuple);
                }
            }
            currentStartTime = currentTime;
        }

        // prune elements
        if (numElements.get() % bucketWidth == 0) {
            //System.out.println("LossyCountingBolt pruning elements");
            pruneElements();
            bucketNumber.incrementAndGet();
        }
    }

    private synchronized List<BucketObject> emitBucket(ConcurrentHashMap<String, BucketObject> bucket) {
        HashMap<String, BucketObject> bucketToEmit = new HashMap<String, BucketObject>();
        List<BucketObject> entries = new LinkedList<>();
        for (String s : bucket.keySet()) {
            BucketObject bo = bucket.get(s);

            // only emit objects that pass the threshold
            if (threshold == -1) {
                bucketToEmit.put(s, bo);
            } else {
                if (bo.getFrequency() >= ((threshold - epsilon) * numElements.get())) {
                    bucketToEmit.put(s, bo);
                }
            }
        }

        if (!bucketToEmit.isEmpty()) {
            entries = new LinkedList<>(bucketToEmit.values());
            Collections.sort(entries);
            if (entries.size() > 100) {
                entries = entries.subList(0, 100);
            }
        }

        return entries;
    }

    private void pruneElements() {
        for (String s : bucket.keySet()) {
            BucketObject bo = bucket.get(s);
            if (bo.getFrequency() + bo.getDelta() <= bucketNumber.get()) {
                bucket.remove(s);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tags", "time"));
    }

}

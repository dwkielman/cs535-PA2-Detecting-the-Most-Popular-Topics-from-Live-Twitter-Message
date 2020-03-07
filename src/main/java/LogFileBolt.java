
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class LogFileBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private String outputPath;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public LogFileBolt(String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String tags = tuple.getStringByField("tags");
        long time = tuple.getLongByField("time");
        Date date = new Date(time);

        try {
            //System.out.println("Writing to the file " + outputPath);
            FileWriter fw = new FileWriter(outputPath, true);
            BufferedWriter bw = new BufferedWriter(fw);
            String log = "<" + dateFormat.format(date) + ">" + tags;
            bw.write(log);
            bw.newLine();
            bw.flush();
            outputCollector.emit(tuple, new Values(log));
            outputCollector.ack(tuple);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("log"));
    }
}

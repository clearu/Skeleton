package clearUTSA.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.io.*;

public class RabbitMQSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    // file read here
    while((line = bufferedReader.readLine()) != null){
      Double data = Double.parseDouble(line);
      _collector.emit(new Values(data));
    }
    bufferedReader.close();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("data"));
  }

}

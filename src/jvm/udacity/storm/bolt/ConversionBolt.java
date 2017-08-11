package udacity.storm.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import java.util.Map;

/**
 * A bolt that converts Celsius to Fahenheit
 */
public class ConversionBolt extends BaseRichBolt {
  // To output tuples from this bolt to the next stage bolts, if any
  private OutputCollector collector;

  @Override
  public void prepare( Map                     map,
                       TopologyContext         topologyContext,
                       OutputCollector         outputCollector){
    // save the collector for emitting tuples
    collector = outputCollector;
  }

  @Override
  public void execute(Tuple tuple){
    //Syntax to get the word from the 1st column of incoming tuple
    Double rawData = tuple.getDouble(0);
    Double convertedData = rawData*(9.00/5.00) + 32.00;
    collector.emit(new Values(convertedData));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer){
    outputFieldsDeclarer.declare(new Fields("converted-data"));
  }
}

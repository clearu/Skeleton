package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import backtype.storm.utils.Utils;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import udacity.storm.spout.TestSpout;
import udacity.storm.bolt.ConversionBolt;
import udacity.storm.bolt.MySQLBolt;

/**
 * This topology demonstrates how to convert a stream of Celsius data into
 * Fahenheit.
 */
public class TempConversionTopology {

  private TempConversionTopology() { }

  /**
   * A bolt that converts Celsius to Fahenheit
   */
  static class ConversionBolt extends BaseRichBolt {
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

  public static void main(String[] args) throws Exception{
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("temp-spout", new TestSpout(), 2);
    builder.setBolt("conversion-bolt", new ConversionBolt(), 3).shuffleGrouping("temp-spout");
    builder.setBolt("report-bolt", new MySQLBolt(), 5).globalGrouping("conversion-bolt");

    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0){
      // run it in a live cluster
      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }else{
      // run it in a simulated local cluster
      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("word-count", conf, builder.createTopology());

      //**********************************************************************
      // let the topology run for 30 seconds. note topologies never terminate!
      Thread.sleep(30000);
      //**********************************************************************

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}

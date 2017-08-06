package clearUTSA.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.task.TopologyContext;

import backtype.storm.utils.Utils;

import clearUTSA.storm.spout.RabbitMQSpout;
import clearUTSA.storm.bolt.ConversionBolt;
import clearUTSA.storm.bolt.MySQLBolt;

/**
 * This topology demonstrates how to convert a stream of Celsius data into
 * Fahenheit.
 */
public class TempConversionTopology {

  private TempConversionTopology() { }

  public static void main(String[] args) throws Exception{
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("temp-spout", new RabbitMQSpout(), 2);
    builder.setBolt("conversion-bolt", new ConversionBolt(), 15).shuffleGrouping("temp-spout");
    builder.setBolt("report-bolt", new MySQLBolt(), 1).globalGrouping("conversion-bolt");

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

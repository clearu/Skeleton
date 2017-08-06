package clearUTSA.storm.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;

import backtype.storm.utils.Utils;

/**
 * A bolt that prints the converted data to a txt file
 */
static class MySQLBolt extends BaseRichBolt{
  String fileNameOut = "output.txt";
  FileWriter fileWriter = new FileWriter(fileNameOut);
  BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

  @Override
  public void prepare(Map                     map,
                      TopologyContext         topologyContext,
                      OutputCollector         outputCollector){
    /**                                         **/
    /*                                           */
    /*  Initialize driver, url, and              */
    /*  connection                               */
    /*                                           */
    /**                                         **/
  }

  @Override
  public void execute(Tuple tuple){
    // access the first resulting data from tuple column 'converted-data'
    Double result = tuple.getStringByField("converted-data");
    /**                                         **/
    /*                                           */
    /*  Communicate with mySQL Database          */
    /*                                           */
    /**                                         **/
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // nothing to add - since it is the final bolt
  }
}

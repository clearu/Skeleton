package udacity.storm.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import java.util.Map;
import java.util.HashMap;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;
import backtype.storm.utils.Utils;


import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;


/**
 * A bolt that prints the converted data to a txt file
 */
public class MySQLBolt extends BaseRichBolt{



  @Override
  public void prepare(Map                     map,
                      TopologyContext         topologyContext,
                      OutputCollector         outputCollector){

  }

  @Override
  public void execute(Tuple tuple){
    // access the first resulting data from tuple column 'converted-data'
    Double result = tuple.getDouble(0);
    try {
      Class.forName("com.mysql.jdbc.Driver");

      Connection connect = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?verifyServerCertificate=false&useSSL=true","root","root");
      PreparedStatement preparedStatement = connect.prepareStatement("insert into data (temp) values (?)");
      preparedStatement.setDouble(1, result);

      preparedStatement.executeUpdate();

    } catch (Exception e) {

    throw new RuntimeException(e);
    }


  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // nothing to add - since it is the final bolt
  }
}

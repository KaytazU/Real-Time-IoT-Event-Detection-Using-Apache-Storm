/*
A bolt class that generates hash keys for data points in hyperspace

Created by Umuralp Kaytaz

*/

package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import udacity.storm.SensorData;
import udacity.storm.Key;
import udacity.storm.Moments;
import udacity.storm.KeyGroupedData;

import java.lang.NullPointerException;

/**
 * A bolt that creates hash keys
 */
public class KeyGroupedHashingBolt1 extends BaseRichBolt
{
  // To emit tuples from this bolt to the local dist calc bolt
  OutputCollector collector;
  //For storing received sensor data
  private List<SensorData> inputSensorData;

  //private List inputSensorData;
  //For storing moment values
  private List<Moments> momentValueList;
  //For storing multiple keys
  private List<Key> hashKeyList;
  //For storing data group that will be emitted
  private List<KeyGroupedData> allSensorData;
  //storing key that will be emitted
  private Key emitKey;
  //for storing sensor data list that will be emitted
  private List<SensorData> emitDataGroup;
  //For storing size
  private int inputSize;

  private String componentID="ANID";

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
    componentID = topologyContext.getThisComponentId();
  }

  @Override
  public void execute(Tuple tuple)
  {
    try{
    //Get the tuple values
    inputSensorData=(List<SensorData>)tuple.getValue(0);

    //store size
    inputSize=inputSensorData.size();
    //System.out.println("input size is: " + inputSize);
    //Get the moments of each sensor data
    momentValueList=new ArrayList<Moments>(inputSize);
    for(int i=0; i< inputSize; i++){
    SensorData sensorData=inputSensorData.get(i);
    //System.out.println("sensor data iteration");
    //System.out.println("Printing sensor data");
    //System.out.println(sensorData.toString());
    Moments sensorMoments=sensorData.getMoments();
    momentValueList.add(sensorMoments);
    //System.out.println("Printing moments........................");
    //System.out.println(sensorMoments.toString());
    }

    //System.out.println("Key generation process begins........................");
    //generateKeys
    hashKeyList=Key.generateKeys(momentValueList);
    //compress keys
    //System.out.println("Keys have been generated ............!!!!");
    hashKeyList=Key.compressKeysInList(hashKeyList);

    //System.out.println("Keys have been compressed............!!!!");
    //Partition sensor data based on keys and getting a cumulative list

    allSensorData=new ArrayList<KeyGroupedData>();
    allSensorData=KeyGroupedData.GroupData(hashKeyList,inputSensorData);
    //Partition KeyGroupData list for transmission
    //System.out.println("Data groups have been generated...........!!!!");
    for(int i=0; i< allSensorData.size(); i++){
      //get KeyGroupedData
      KeyGroupedData aGroup=allSensorData.get(i);
      //get related key
      emitKey=aGroup.getGroupKey();
      //get related data
      emitDataGroup=aGroup.getGroupData();
      //forward keys and related data
      collector.emit(new Values(emitKey,emitDataGroup));
      //System.out.println("A successful transmission has happened.........:D");
      //aGroup.dataPrint();
    }
  }catch(NullPointerException e ){
    System.out.println("No input to bolt: "+ componentID);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // tell storm the schema of the output tuple for this bolt
    declarer.declare(new Fields("emitted-key","emitted-data-group"));
  }


}

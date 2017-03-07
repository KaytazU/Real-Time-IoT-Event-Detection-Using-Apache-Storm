/*
A bolt class that gets event data then partitons the events

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
import udacity.storm.EventData;

import java.lang.NullPointerException;

/**
 * A bolt that partitions events
 */
public class EventPartitioningBolt extends BaseRichBolt
{
  // To emit tuples from this bolt to the local dist calc bolt
  OutputCollector collector;
  private String componentID="ANID";
  private List<EventData> eventDataList;
  private int receivedListSize;
  private int NUM_CLUSTERS=5;
  private ArrayList<SensorData>[] sensorDataGroupArray;
  private List<SensorData> listToEmit;
  private int clusterIndex;


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
    eventDataList=(List<EventData>)tuple.getValue(0);
    receivedListSize=eventDataList.size();
    sensorDataGroupArray = (ArrayList<SensorData>[])new ArrayList[NUM_CLUSTERS];
    //initializing lists
    for(int i=0; i<NUM_CLUSTERS;i++){
      sensorDataGroupArray[i]=new ArrayList<SensorData>();
    }
    //adding received event data to their belonging lists
    for(int i=0;i<receivedListSize;i++){
      EventData anEventData=eventDataList.get(i);
      int clusterID=anEventData.getID();
      SensorData aSensorData=anEventData.getData();
      sensorDataGroupArray[clusterID].add(aSensorData);
    }

    for(int i=0; i<NUM_CLUSTERS;i++){
      listToEmit=sensorDataGroupArray[i];
      clusterIndex=i;

      //System.out.println("$$$$$$$$$$$$$$$$$$$$Printing partitioned event data ");
      //for(int j=0;j<listToEmit.size();j++){
      //  System.out.println("cluster index : " + clusterIndex );
      //  System.out.println(listToEmit.get(j).toString());
      //}

      //emitting clusterIndex int & sensorData list
      collector.emit(new Values(clusterIndex,listToEmit));
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    // tell storm the schema of the output tuple for this bolt
    declarer.declare(new Fields("emitted-event-index","emitted-event-data-group"));
  }


}

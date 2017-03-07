/*

Created by Umuralp Kaytaz
A bolt for drift detection

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
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;

import udacity.storm.SensorData;
import udacity.storm.DriftData;
import udacity.storm.RelativeEntropy;

public class DriftDetectionBolt extends BaseRichBolt
{
OutputCollector collector;
private String componentId="null";

private int receivedClusterID;
private List<SensorData> receivedData;
//private boolean drift=false;
private List<SensorData> list1=new ArrayList<SensorData>();
private List<SensorData> list2=new ArrayList<SensorData>();
private List<DriftData> driftDataList=new ArrayList<DriftData>();
private List<RelativeEntropy> relativeEntrophyValues=new ArrayList<RelativeEntropy>();

  @Override
  public void prepare(
      Map                     map,
      TopologyContext         topologyContext,
      OutputCollector         outputCollector)
  {
    // save the output collector for emitting tuples
    collector = outputCollector;
    componentId = topologyContext.getThisComponentId();
  }


  @Override
  public void execute(Tuple tuple)
  {
    //get input
    receivedClusterID=(int)tuple.getValue(0);
    receivedData=(List<SensorData>)tuple.getValue(1);
    //filling our lists for storing consequtive data
    if(!list1.isEmpty()){
      list2=receivedData;
    }else{
      list1=receivedData;
    }


    //if we have two consequtive sensordata values
    if(!list1.isEmpty() && !list2.isEmpty()){

      //System.out.println("I have two consequent data");
      //build drift data list from emitted sensor data lists
      driftDataList=DriftData.buildDriftData(list1,list2);
      if(driftDataList !=null){
        relativeEntrophyValues=RelativeEntropy.calculateRelativeEntrophy(driftDataList);
        collector.emit(new Values(relativeEntrophyValues));
        //for testing
        //System.out.println("printing relative entropy values $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
        //for(int i=0;i< relativeEntrophyValues.size();i++){
        //  System.out.println(relativeEntrophyValues.get(i).toString());
        //    }
        for(int i=0;i<relativeEntrophyValues.size();i++){
          RelativeEntropy anEntropy=relativeEntrophyValues.get(i);
          double anID=anEntropy.getSensorID();
          double theEntropyValue=anEntropy.getEntropyValue();
          if(theEntropyValue>= 0.1){
            System.out.println("There is significant drift detected in sensor with ID " + anID);
            }
          }
        }
      //cleaning lists
      list1=new ArrayList<SensorData>();
      list2=new ArrayList<SensorData>();
    }


  }



  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("relative-entropy-values"));
  }

}

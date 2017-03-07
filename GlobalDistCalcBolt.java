/*
Created by Umuralp Kaytaz

Global distance calculation Bolt
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


public class GlobalDistCalcBolt extends BaseRichBolt
{

  private int calcLimit=5;
  private String componentId="null";
  private SensorData receivedCentroid;
  private List<SensorData> receivedDataGroup;
  private int receivedDataSize;
  private List<SensorData> receivedCentroidGroup=new ArrayList<SensorData>();
  private int boltsCurrentCentroidStorage;
  private List<SensorData> boltData=new ArrayList<SensorData>();
  private SensorData centralCentroidData;
  private double eucDist;
  OutputCollector collector;

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
    receivedCentroid=(SensorData)tuple.getValue(0);
    receivedDataGroup=(List<SensorData>)tuple.getValue(1);
    //System.out.println("Received centroid .......");
    //System.out.println(receivedCentroid.toString());

    //accumulating all data
    receivedDataSize=receivedDataGroup.size();
    for(int i=0; i< receivedDataSize;i++){
      boltData.add(receivedDataGroup.get(i));
    }
    //accumulating received centroids
    receivedCentroidGroup.add(receivedCentroid);
    //getting number of centroids
    boltsCurrentCentroidStorage=receivedCentroidGroup.size();
    //System.out.println("Number of centroids in....... " + componentId +"   is ......" + boltsCurrentCentroidStorage);

    if(boltsCurrentCentroidStorage >= calcLimit){
      //System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Storage has now enough centroids data for calculation in bolt:  " + componentId);
      double[] distArray=new double[boltsCurrentCentroidStorage];
      double totalPointDist=0;
      double averagePointDistance=0;
      int smallestIndex=0;
      double smallestDistance=10000;
      //finding average distance of each moments to other moments in 4-dimensional space
      for(int i=0; i<boltsCurrentCentroidStorage;i++){
        //iterating over each data for each data
        for(int j=0; j<boltsCurrentCentroidStorage;j++){
          eucDist=calculateDistance(receivedCentroidGroup.get(i),receivedCentroidGroup.get(j));
          //System.out.println("first data " + boltData.get(i).toString());
          //System.out.println("second data " + boltData.get(j).toString());
          //System.out.println("distance between them " + eucDist);
          totalPointDist+=eucDist;
        }
        //After finding total dist for each data
        averagePointDistance=totalPointDist/boltsCurrentCentroidStorage;
        distArray[i]=averagePointDistance;

        if(averagePointDistance < smallestDistance){
          //System.out.println("Average point dist " + averagePointDistance);
          //System.out.println("Smallest dist "+ smallestDistance);
          smallestDistance=averagePointDistance;
          smallestIndex=i;
              }
        //restart dist sum count and smallest dist's initial value
        totalPointDist=0;

      }
      //System.out.println("Printing Distance array ........./!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!$$$$$$$$$$$$$$$");
    //  for(int i=0; i<distArray.length;i++){
      //  System.out.println(distArray[i]);
    //  }
      //System.out.println("Smallest Index is !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! " + smallestIndex);
      //Index i of bolts current storage has the most central moments
      centralCentroidData=receivedCentroidGroup.get(smallestIndex);
      //Sending centroid and whole data around it
      collector.emit(new Values(centralCentroidData,boltData));

      //System.out.println("Printing central centroid ..........................!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!$$$$$$$$$$$$$$$$$$$$$");
      //for testing
      //System.out.println(centralCentroidData.toString());
      //System.out.println("Printing bolt data ..........................!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!$$$$$$$$$$$$$$$$$$$$$");
      //for(int i=0; i<boltData.size();i++){
      //System.out.println(boltData.get(i).toString());
      //}
      //dont forget to clean bolt storage after whole process
      boltData=new ArrayList<SensorData>();
      receivedCentroidGroup=new ArrayList<SensorData>();
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("central-centroid-data","all-bolt-data"));
  }
  //ceuc dist calculation
  public static double calculateDistance(SensorData data1, SensorData data2)
      {
        Moments moments1=data1.getMoments();
        Moments moments2=data2.getMoments();
        double Sum = 0.0;
        for(int i=0;i<4;i++) {
           Sum = Sum + Math.pow((moments1.getRM(i)-moments2.getRM(i)),2.0);
        }
        return Math.sqrt(Sum);
      }
}

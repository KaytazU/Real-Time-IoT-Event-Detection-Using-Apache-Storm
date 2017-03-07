/*
Created by Umuralp Kaytaz

Local distance calculation Bolt
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


public class LocalDistCalcBolt extends BaseRichBolt
{

  private int calcLimit=30;
  private String componentId="null";
  private Key receivedKey;
  private List<SensorData> receivedDataGroup;
  private int receivedDataSize;
  private int boltsCurrentStorage;
  private List<SensorData> boltData=new ArrayList<SensorData>();
  private SensorData centroidData;
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
    receivedKey=(Key)tuple.getValue(0);
    receivedDataGroup=(List<SensorData>)tuple.getValue(1);
    receivedDataSize=receivedDataGroup.size();

    for(int i=0; i< receivedDataSize;i++){
      boltData.add(receivedDataGroup.get(i));
    }
    boltsCurrentStorage=boltData.size();

    if(boltsCurrentStorage >= calcLimit){
      //System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Storage has now enough sensor data for calculation in bolt:  " + componentId);
      double[] distArray=new double[boltsCurrentStorage];
      double totalPointDist=0;
      double averagePointDistance=0;
      int smallestIndex=0;
      double smallestDistance=1000;
      //finding average distance of each moments to other moments in 4-dimensional space
      for(int i =0; i<boltsCurrentStorage;i++){
        //iterating over each data for each data
        for(int j=0; j<boltsCurrentStorage;j++){
          eucDist=calculateDistance(boltData.get(i),boltData.get(j));
          //System.out.println("first data " + boltData.get(i).toString());
          //System.out.println("second data " + boltData.get(j).toString());
          //System.out.println("distance between them " + eucDist);
          totalPointDist+=eucDist;
        }
        //After finding total dist for each data
        averagePointDistance=totalPointDist/boltsCurrentStorage;
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
      centroidData=boltData.get(smallestIndex);
      //Sending centroid and whole data around it
      collector.emit(new Values(centroidData,boltData));

      //System.out.println("Printing centroid ..........................!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!$$$$$$$$$$$$$$$$$$$$$");
      //for testing
      //System.out.println(centroidData.toString());
      //System.out.println("Printing bolt data ..........................!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!$$$$$$$$$$$$$$$$$$$$$");
      //for(int i=0; i<boltsCurrentStorage;i++){
      //System.out.println(boltData.get(i).toString());
      //}

      //dont forget to clean bolt storage after whole process
      boltData=new ArrayList<SensorData>();
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("centroid-data","all-bolt-data"));
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

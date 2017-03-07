/*
Created by Umuralp Kaytaz

K-Means clustering bolt for event detection
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
import java.util.Set;
import java.util.HashSet;

import udacity.storm.KMeans;
import udacity.storm.SensorData;
import udacity.storm.Point;
import udacity.storm.Cluster;
import udacity.storm.EventData;


public class FinalClusteringBolt extends BaseRichBolt
{
  //capacity of data storage
  private int capacity=5;
  private String componentId="null";
  private int NUM_CLUSTERS;
  private int NUM_POINTS;
  private int receivedDataSize;
  private int numOfCentroids;
  private List<SensorData> centroids=new ArrayList<SensorData>();
  private List<SensorData> clusterData=new ArrayList<SensorData>();
  private SensorData receivedCentroid;
  private List<SensorData> receivedDataGroup;
  private List<Point>  finalCentroids;
  private List<Cluster> clusters;
  private List<EventData> eventDataList;
  private int clusterID;
  private EventData eventData;
  private SensorData eventsSensorData;
  private List<List<SensorData>> emitDataStorage;
  private List<SensorData> listToEmit;
  // To output tuples from this bolt to the count bolt
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
    //getting the data emitted from the global dist calc bolt
    receivedCentroid=(SensorData)tuple.getValue(0);
    receivedDataGroup=(List<SensorData>)tuple.getValue(1);

    //accumulating received centroids
    centroids.add(receivedCentroid);
    //accumulating all data
    receivedDataSize=receivedDataGroup.size();
    for(int i=0; i< receivedDataSize;i++){
      clusterData.add(receivedDataGroup.get(i));
    }

    //getting number of centroids
    numOfCentroids=centroids.size();

    //if the cluster capacity is reached
    if(numOfCentroids >=capacity){
      //setting number of clusters
      NUM_CLUSTERS=5;
      //get number of points
      NUM_POINTS=clusterData.size();
      //initializing Kmeans
      KMeans kmeans = new KMeans(NUM_CLUSTERS,NUM_POINTS);
      //Creating initial clusters
      kmeans.init(centroids,clusterData);
      //Calculating and getting clusters
      clusters=kmeans.calculate();
      //finding corresponding sensor data
      if(clusters != null){
      eventDataList=EventData.matchSensorDataToClusters(clusters,clusterData);
      //converting to a hash set to have unqie elements
      Set<EventData> uniqueEventData = new HashSet<EventData>(eventDataList);
      //converting the set back to an arraylist
      eventDataList = new ArrayList<EventData>(uniqueEventData);

      collector.emit(new Values(eventDataList));
    }
      //initiating lists
    //emitDataStorage=new ArrayList<List<SensorData>>(NUM_CLUSTERS);
    //for(int i=0; i< NUM_CLUSTERS;i++){

    //  emitDataStorage.set(i,new ArrayList<SensorData>());
    //}
    //System.out.println("$$$$$$$$$$$$$$$Printing cluster data");

    //for(int i=0;i< eventDataList.size();i++){
    //  System.out.println(eventDataList.get(i).toString());
    //}

    //storing sensor data of same clusters in dedicated lists
    //for(int i=0; i<eventDataList.size();i++){
      //taking important things about each event data
    //  eventData=eventDataList.get(i);
    //  clusterID=eventData.getID();
    //  eventsSensorData=eventData.getData();
      //not sure
    //  emitDataStorage.get(clusterID).add(eventsSensorData);
    //    }
    //emitting sensor data list with cluster ID they belong
    //for(int i=0; i<emitDataStorage.size();i++){
    //  listToEmit=emitDataStorage.get(i);
    //  clusterID=i;
    //  collector.emit(new Values(clusterID,listToEmit));
    //  }

      centroids=new ArrayList<SensorData>();
      clusterData=new ArrayList<SensorData>();


    }

  }

  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("event-data-list"));
  }



}

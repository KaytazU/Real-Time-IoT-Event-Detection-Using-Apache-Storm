/*

Created by Umuralp Kaytaz

To represent output of event detection

*/

package udacity.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Arrays;

import udacity.storm.SensorData;
import udacity.storm.Point;
import udacity.storm.Cluster;

public class EventData{

  private SensorData sensorData;
  private int clusterID;
  private Point clusterCentroid;

  public EventData(SensorData sensorData, int clusterID, Point clusterCentroid){
      this.setData(sensorData);
      this.setID(clusterID);
      this.setCentroid(clusterCentroid);
  }

  public EventData(){
      Random _rand=new Random();
      SensorData sensorData=new SensorData();
      Point centroidPoint=new Point();
      this.setData(sensorData);
      this.setID(_rand.nextInt(10));
      this.setCentroid(centroidPoint);
  }

  public void setData(SensorData sensorData) {
      this.sensorData = sensorData;
  }

  public SensorData getData()  {
      return this.sensorData;
  }

  public void setID(int clusterID) {
      this.clusterID = clusterID;
  }

  public int getID()  {
      return this.clusterID;
  }

  public void setCentroid(Point clusterCentroid) {
      this.clusterCentroid = clusterCentroid;
  }

  public Point getCentroid()  {
      return this.clusterCentroid;
  }

  protected static List<EventData> matchSensorDataToClusters(List<Cluster> clusters, List<SensorData> clusterData){
    List<EventData> eventDataList=new ArrayList<EventData>();
    for(int i=0; i<clusters.size();i++){
      //Get info of each cluster
      Cluster c = (Cluster)clusters.get(i);
      List<Point> clusterPoints=c.getPoints();
    	Point centroid=c.getCentroid();
    	int id=c.getId();
      //iterating over each data at cluster
      for(int j=0;j<clusterData.size();j++){
        EventData eventData=new EventData();
        SensorData sensorData=clusterData.get(j);
        Moments sensorMoments=sensorData.getMoments();
        //there is almost no possibility for two double values are exactly same therefore checking only first moment
        double firstMoment=sensorMoments.getRM(0);
        for(int k=0;k<clusterPoints.size();k++){
          Point aPoint=clusterPoints.get(k);
          double pointX=aPoint.getX();
          double d=pointX-firstMoment;
          if(d==0){
            //System.out.print("Found a sensor data match $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
            eventData.setData(sensorData);
            eventData.setID(id);
            eventData.setCentroid(centroid);
            eventDataList.add(eventData);
            }
          }
        }
    }
    return eventDataList;
  }

  public String toString(){
    return "( sensor data: "+sensorData.toString()+", cluster ID is "+clusterID+", cluster centroid is "+clusterCentroid.toString()+")";
  }



}

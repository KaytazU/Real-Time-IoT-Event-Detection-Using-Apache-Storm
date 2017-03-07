/*

Class for representing sensor data

Created by: Umuralp Kaytaz

*/

package udacity.storm;


import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Map;
import java.util.ArrayList;
import java.lang.*;
import java.util.Arrays;

import udacity.storm.Moments;

public class SensorData {

    private double arrivalTime;
    private double sensorID;
    private Moments rawDataMoments;
    private int numOfPossibleIds=100;

    public SensorData(Moments rawDataMoments, double sensorID, double dataTime)
    {
        this.setMoments(rawDataMoments);
        this.setID(sensorID);
        this.setTime(dataTime);
    }

    public SensorData(Moments rawDataMoments, double sensorID)
    {
        this.setMoments(rawDataMoments);
        this.setID(sensorID);
        this.setTime();
    }

    public SensorData(Moments rawDataMoments)
    {
        this.setMoments(rawDataMoments);
        this.setID((double)ThreadLocalRandom.current().nextInt(0,numOfPossibleIds));
        this.setTime();
    }

    public SensorData(){
      Moments randMoments=new Moments();
      this.setMoments(randMoments);
      this.setID((double)ThreadLocalRandom.current().nextInt(0,numOfPossibleIds));
      this.setTime();
    }

    public void setMoments(Moments rawDataMoments) {
        this.rawDataMoments = rawDataMoments;
    }

    public Moments getMoments(){
        return this.rawDataMoments;
    }

    public void setID(double sensorID) {
        this.sensorID = sensorID;
    }

    public double getID()  {
        return this.sensorID;
    }

    public void setTime(double dataTime) {
        this.arrivalTime = dataTime;
    }

    public void setTime() {
        this.arrivalTime = (double)System.currentTimeMillis();
    }

    protected static SensorData generateRandomData(){
      Moments newMoments=Moments.generateRandomMoments();
      //System.out.println("new moments are generated");
      //System.out.println(newMoments.toString());
      SensorData newData=new SensorData(newMoments);
      return newData;
    }

    public static List generateMultipleData(int size){
      List multipleSensorData=new ArrayList<SensorData>(size);
      for(int i = 0; i < size; i++) {
        SensorData aNewData=generateRandomData();
        //System.out.println("new sensor data is generated");

        //System.out.println(aNewData.toString());
        multipleSensorData.add(aNewData);
      }
      return multipleSensorData;
    }

    public String toString(){
    return "("+rawDataMoments+","+sensorID+","+arrivalTime+")";
    }

}

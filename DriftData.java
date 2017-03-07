 /*

Class for representing drift data

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
import java.lang.Math;
import java.util.Set;
import java.util.HashSet;

import udacity.storm.SensorData;
import udacity.storm.Moments;
import udacity.storm.RelativeEntropy;

public class DriftData{

private double sensorID;
private Moments momentsAtTime1;
private Moments momentsAtTime2;

  public DriftData (double sensorID, Moments moments1,Moments moments2){
      this.setSensorID(sensorID);
      this.setFirstMoments(moments1);
      this.setSecondMoments(moments2);
  }

  public void setSensorID(double sensorID) {
      this.sensorID = sensorID;
  }

  public double getSensorID()  {
      return this.sensorID;
  }

  public void setFirstMoments(Moments firstMoments){
      this.momentsAtTime1=firstMoments;
  }

  public Moments getFirstMoments(){
      return this.momentsAtTime1;
  }

  public void setSecondMoments(Moments secondMoments){
      this.momentsAtTime2=secondMoments;
  }

  public Moments getSecondMoments(){
      return this.momentsAtTime2;
  }

  public String toString(){
    return "(" + " Sensor ID is "+ sensorID+ " first moments "+momentsAtTime1.toString()+" second moments " + momentsAtTime2.toString()+" )";
  }

  protected static List<DriftData> buildDriftData(List<SensorData> list1, List<SensorData> list2){
    List<DriftData> driftDataList=new ArrayList<DriftData>();
    List<SensorData> listAtTime1=new ArrayList<SensorData>();
    List<SensorData> listAtTime2=new ArrayList<SensorData>();
    listAtTime1=list1;
    listAtTime2=list2;

    //finding sensors that have data at consequtive time frames
    for(int i=0; i<listAtTime1.size();i++){
        SensorData data1=listAtTime1.get(i);
        Moments moments1=data1.getMoments();
        double ID1=data1.getID();
        for(int j=0;j<listAtTime2.size();j++){
          SensorData data2=listAtTime2.get(j);
          Moments moments2=data2.getMoments();
          double ID2=data2.getID();
          double d=ID1-ID2;
          //When same sensor has data in two consequtive time frames
            if(d==0){
              DriftData driftData=new DriftData(ID1,moments1,moments2);
              //System.out.println("$$$$$$$$$$$$Printing drift data");
              //System.out.println(driftData.toString());
              driftDataList.add(driftData);
            }
          }
        }

      if(driftDataList!=null){
        //converting to a hash set to have unqie elements
        Set<DriftData> uniqueDriftData = new HashSet<DriftData>(driftDataList);
        //converting the set back to an arraylist
        driftDataList = new ArrayList<DriftData>(uniqueDriftData);

        //getting unique sensor drift data
        List<Double> IDList=new ArrayList<Double>();
        List<DriftData> uniqueDriftDataList=new ArrayList<DriftData>();

        for(int i=0;i<driftDataList.size();i++){
            DriftData aData=driftDataList.get(i);
            double anID=aData.getSensorID();
            if(!IDList.contains(anID)){
              IDList.add(anID);
              uniqueDriftDataList.add(aData);
            }
        }

        //test
        //System.out.println("$$$$$$$$$$$$Printing unique drift data");
        //for(int k=0;k<uniqueDriftDataList.size();k++){
        //  System.out.println(uniqueDriftDataList.get(k).toString());
        //}
      return uniqueDriftDataList;
      }
    return null;
    }//end of buildDriftData


}

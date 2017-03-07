/*

Class for representing relative entropy

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

import udacity.storm.SensorData;
import udacity.storm.Moments;
import udacity.storm.RelativeEntropy;

public class RelativeEntropy{

  private double sensorID;
  private double entropyValue;

  public RelativeEntropy(double sensorID,double entropyValue){
    this.setSensorID(sensorID);
    this.setEntropyValue(entropyValue);
  }

  public RelativeEntropy(){
    Random _rand=new Random();
    this.setSensorID(_rand.nextDouble());
    this.setEntropyValue(_rand.nextDouble());
  }

  public void setSensorID(double sensorID){
    this.sensorID=sensorID;
  }
  public double getSensorID(){
    return this.sensorID;
  }
  public void setEntropyValue(double entropyValue){
    this.entropyValue=entropyValue;
  }
  public double getEntropyValue(){
    return this.entropyValue;
  }

  public String toString(){
    return "( " + " relative entropy for " + sensorID + " is " + entropyValue + " ) ";
  }

    protected static List<RelativeEntropy> calculateRelativeEntrophy(List<DriftData> driftDataList){
      //to store values during calculation
        double momentValue1;
        double momentValue2;
        double momentSum1;
        double momentSum2;
        int numOfDriftData=driftDataList.size();
        int numOfRM=4;
        List<RelativeEntropy> relativeEntropyValues=new ArrayList<RelativeEntropy>();
        //iterate over each drift data
        for(int i=0;i< numOfDriftData;i++){
            //to store P and Q
            double[] Parray=new double[4];
            double[] Qarray=new double[4];
            RelativeEntropy RE=new RelativeEntropy();
            //get a drift data
            DriftData theDriftData=driftDataList.get(i);
            //get sensor ID
            double sensorID=theDriftData.getSensorID();
            //get its moments
            Moments firstMoments=theDriftData.getFirstMoments();
            Moments secondMoments=theDriftData.getSecondMoments();

            momentSum1=secondMoments.getRM(0)+secondMoments.getRM(1)+secondMoments.getRM(2)+secondMoments.getRM(3);
            momentSum2=firstMoments.getRM(0)+firstMoments.getRM(1)+firstMoments.getRM(2)+firstMoments.getRM(3);

            //iterate over RM of moments to calculate array values used in relative entropy calculation
            for(int j=0;j<numOfRM;j++){
                momentValue1=secondMoments.getRM(j);
                Parray[j]=momentValue1/momentSum1;
                momentValue2=firstMoments.getRM(j);
                Qarray[j]=momentValue2/momentSum2;
              }
            //calculate relative entropy
            double totalSum=0;
            for(int j=0;j<numOfRM;j++){
                double sum=Parray[j]*Math.log(Parray[j]/Qarray[j]);
                totalSum +=sum;
            }
            //adding the RE value to the list for each drift data
            RE.setSensorID(sensorID);
            RE.setEntropyValue(totalSum);
            relativeEntropyValues.add(RE);
          }

          return relativeEntropyValues;
      }

}

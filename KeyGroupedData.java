/*

Class for key grouped Data

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

import udacity.storm.Key;
import udacity.storm.SensorData;

public class KeyGroupedData {
//defining key digits
  private List<SensorData> sensorDataList;
  private Key groupKey;
//constructor
  public KeyGroupedData(Key key,List<SensorData> sensorDataList){
    this.setGroupKey(key);
    this.setGroupData(sensorDataList);
  }
  //to get data from group
  public List getGroupData()  {
      return this.sensorDataList;
  }
  //to set data to group
  public void setGroupData(List dataList) {
        this.sensorDataList = dataList;
  }
  //to get key of group
  public Key getGroupKey(){
    return this.groupKey;
  }
  //to set key of group
  public void setGroupKey(Key key) {
        this.groupKey=key;
  }
  //To create data groups given the list of keys and their corresponding sensor data packets
  protected static List<KeyGroupedData> GroupData(List<Key> keyList, List<SensorData> sensorDataList){
    //Create referans key, sensorData and List to store KeyGroupedData
    Key dataKey=new Key();
    SensorData relatedData=new SensorData();

    List allSensorData=new ArrayList<KeyGroupedData>();
    //Create referance keys
    Key key1=new Key(false,false,false,false);
    Key key2=new Key(false,true,false,false);
    Key key3=new Key(true,false,false,false);
    Key key4=new Key(true,true,false,false);
    //Create lists for data storage
    List toKey1=new ArrayList<SensorData>();
    List toKey2=new ArrayList<SensorData>();
    List toKey3=new ArrayList<SensorData>();
    List toKey4=new ArrayList<SensorData>();
    //Get list size
    int size=keyList.size();
    //Iterate over each key and corresponding data individually
    for(int i=0; i<size;i++){
      dataKey=keyList.get(i);
      relatedData=sensorDataList.get(i);
      //System.out.println(dataKey.toString());
      //put data to its belonging list
      if(dataKey.compareKeys(key1)){
        toKey1.add(relatedData);
      }else if(dataKey.compareKeys(key2)){
        toKey2.add(relatedData);
      }else if(dataKey.compareKeys(key3)){
        toKey3.add(relatedData);
      }else if(dataKey.compareKeys(key4)){
        toKey4.add(relatedData);
      }else{
        System.out.println("Key cannot be resolved, there is an error");
      }
    }
    //Generating Key grouped data
    KeyGroupedData key1Group=new KeyGroupedData(key1,toKey1);
    KeyGroupedData key2Group=new KeyGroupedData(key2,toKey2);
    KeyGroupedData key3Group=new KeyGroupedData(key3,toKey3);
    KeyGroupedData key4Group=new KeyGroupedData(key4,toKey4);
    //Adding data groups to the cumulative list
    allSensorData.add(key1Group);
    allSensorData.add(key2Group);
    allSensorData.add(key3Group);
    allSensorData.add(key4Group);
    return allSensorData;
  }
  public void dataPrint() {
    String outputString= "the key is "+ this.getGroupKey()+" and the data are ";
    System.out.println(outputString);
    for(int i=0; i< this.getGroupData().size(); i++){
      SensorData aData=(SensorData)this.getGroupData().get(i);
      String dataString= aData.toString();
      System.out.println(dataString);
      }
    }

}

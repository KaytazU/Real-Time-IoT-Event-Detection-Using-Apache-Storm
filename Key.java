/*

Class for hash key

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

public class Key{
//defining key digits
  private boolean KD1= false;
  private boolean KD2= false;
  private boolean KD3= false;
  private boolean KD4= false;

//constructors
  public Key(boolean KD1,boolean KD2,boolean KD3,boolean KD4)
  {
      this.setKD1(KD1);
      this.setKD2(KD2);
      this.setKD3(KD3);
      this.setKD4(KD4);
  }

  public Key(){
    Random r=new Random();
    this.setKD1(r.nextBoolean());
    this.setKD2(r.nextBoolean());
    this.setKD3(r.nextBoolean());
    this.setKD4(r.nextBoolean());
  }

//Getters & Setters
//set digit at index
public void setKD(boolean KD, int Index) {
  if(Index==0){
    setKD1(KD);
  } else if(Index==1){
    setKD2(KD);
  } else if(Index==2){
    setKD3(KD);
  } else if(Index==3){
    setKD4(KD);
  }else{
    System.out.println("Wrong key index");
  }
}
//get digit at index
public boolean getKD(int Index) {
  if(Index==0){
    return getKD1();
  } else if(Index==1){
    return getKD2();
  } else if(Index==2){
    return getKD3();
  } else if(Index==3){
    return getKD4();
  }else{
    System.out.println("Wrong key index");
    return false;
    }
  }

  public void setKD1(boolean KD1) {
      this.KD1 = KD1;
  }

  public boolean getKD1()  {
      return this.KD1;
  }

  public void setKD2(boolean KD2) {
      this.KD2 = KD2;
  }

  public boolean getKD2()  {
      return this.KD2;
  }

  public void setKD3(boolean KD3) {
      this.KD3 = KD3;
  }

  public boolean getKD3()  {
      return this.KD3;
  }

  public void setKD4(boolean KD4) {
      this.KD4 = KD4;
  }

  public boolean getKD4()  {
      return this.KD4;
  }


  //method for generating a key list given moment list
  protected static List generateKeys(List<Moments> momentValueList){
      //Initialize boolean hash key list
      List keyList=new ArrayList<Key>();
      //list size
      int size=momentValueList.size();
      //key generation process
      for(int m=0; m<size; m++){
        Moments moments=momentValueList.get(m);
        Key key=new Key();
        //Creating hashkey
        for(int i=0;i<4;i++){
          //divinding each dimension to equal partitions
          if(moments.getRM(i)<0.5){
            key.setKD(false,i);
          }else{
            key.setKD(true,i);
            }
          }
          //System.out.println("printing key ................");
          //System.out.println(moments.toString());
          //System.out.println(key.toString());
          //adding key to the key list
          keyList.add(key);
        }
        return keyList;
    }

    //method for grouping keys
  protected static Key compressHashKey(Key hashKey){
      Key newHashKey=new Key();
      //Grouping hash keys to create less bolts in future steps
      //0000,0001,0010,0011 -> 0000
      if(hashKey.getKD(0)==false && hashKey.getKD(1)==false){
        newHashKey=new Key(false,false,false,false);
      }
      //0100,0101,0110,0111 -> 0100
      if(hashKey.getKD(0)==false && hashKey.getKD(1)==true){
        newHashKey=new Key(false,true,false,false);
      }
      //1000,1001,1010,1011 -> 1000
      if(hashKey.getKD(0)==true && hashKey.getKD(1)==false){
        newHashKey=new Key(true,false,false,false);
      }
      //1100,1101,1110,1111 -> 1100
      if(hashKey.getKD(0)==true && hashKey.getKD(1)==true){
        newHashKey=new Key(true,true,false,false);
      }
      //System.out.println("old key " + hashKey.toString());
      //System.out.println("new key " + newHashKey.toString());
      return newHashKey;
    }

    //method for compressing keys
    protected static List compressKeysInList(List<Key> keyList){
      //initializing new key list
      List newKeyList=new ArrayList<Key>();
      //getting old list size
      int size=keyList.size();
      //iterating over keys
      for(int i=0;i<size;i++){
      newKeyList.add(compressHashKey(keyList.get(i)));
    }
    return newKeyList;
  }

  public String toString() {
    	return "("+KD1+","+KD2+","+KD3+","+KD4+")";
    }

    public boolean compareKeys(Key anotherKey) {
    String anotherKeyString = anotherKey.toString();
    return this.toString().equals(anotherKeyString);
  }

}

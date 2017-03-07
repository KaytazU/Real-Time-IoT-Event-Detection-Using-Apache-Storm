/*

Class for Data distribution moments

Created by: Umuralp Kaytaz

RM1=First order moment  =>Mean
RM2=Second order moment =>Variance
RM3=Third order moment  =>Skewness
RM4=Fourth order moment =>Kurtosis

*/



package udacity.storm;


import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Map;
import java.util.ArrayList;
import java.lang.*;
import java.util.Arrays;

public class Moments {

  //assuming distribution is represented by 4 raw data Moments
  private double RM1=0;
  private double RM2=0;
  private double RM3=0;
  private double RM4=0;

  public Moments(double RM1,double RM2,double RM3,double RM4)
  {
      this.setRM1(RM1);
      this.setRM2(RM2);
      this.setRM3(RM3);
      this.setRM4(RM4);
  }

  public Moments(){
    Random r=new Random();
    this.setRM1(r.nextDouble());
    this.setRM2(r.nextDouble());
    this.setRM3(r.nextDouble());
    this.setRM4(r.nextDouble());
  }

  //Getters & Setters
  //set digit at index
  public void setRM(double RM, int Index) {
    if(Index==0){
      setRM1(RM);
    } else if(Index==1){
      setRM2(RM);
    } else if(Index==2){
      setRM3(RM);
    } else if(Index==3){
      setRM4(RM);
    }else{
      System.out.println("Wrong key index");
    }
  }
  //get digit at index
  public double getRM(int Index) {
    double value=0;
    if(Index==0){
      return getRM1();
    } else if(Index==1){
      return getRM2();
    } else if(Index==2){
      return getRM3();
    } else if(Index==3){
      return getRM4();
    }else{
      System.out.println("Wrong key index");
      return value;
    }
  }

  public void setRM1(double RM1) {
        this.RM1 = RM1;
    }

  public double getRM1()  {
        return this.RM1;
    }

  public void setRM2(double RM2) {
        this.RM2 = RM2;
    }

  public double getRM2()  {
        return this.RM2;
    }

  public void setRM3(double RM3) {
        this.RM3 = RM3;
    }

  public double getRM3()  {
        return this.RM3;
    }

  public void setRM4(double RM4) {
        this.RM4 = RM4;
    }

  public double getRM4()  {
        return this.RM4;
    }
  public static Moments generateRandomMoments(){
      Random _rand=new Random();
      double RM1=_rand.nextDouble();
      double RM2=_rand.nextDouble();
      double RM3=_rand.nextDouble();
      double RM4=_rand.nextDouble();
      return new Moments(RM1,RM2,RM3,RM4);
    }

  public static List generateMultipleMoments(int size){
      List momentList = new ArrayList<Moments>(size);
      for(int i = 0; i < size; i++) {
        momentList.add(Moments.generateRandomMoments());
      }
      return momentList;
    }

  public String toString() {
    	return "("+RM1+","+RM2+","+RM3+","+RM4+")";
    }

}

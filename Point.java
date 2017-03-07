package udacity.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Arrays;

import udacity.storm.Moments;
import udacity.storm.SensorData;

public class Point {


    private double x = 0;
    private double y = 0;
    private double z = 0;
    private double t = 0;
    private int cluster_number = 0;

    public Point(double x, double y, double z, double t)
    {
        this.setX(x);
        this.setY(y);
        this.setZ(z);
        this.setT(t);
    }

    public Point(){
      Random _rand=new Random();
      this.setX(_rand.nextDouble());
      this.setY(_rand.nextDouble());
      this.setZ(_rand.nextDouble());
      this.setT(_rand.nextDouble());

    }

    public void setX(double x) {
        this.x = x;
    }

    public double getX()  {
        return this.x;
    }

    public void setY(double y) {
        this.y = y;
    }

    public double getY() {
        return this.y;
    }
    public void setZ(double z) {
        this.z = z;
    }

    public double getZ()  {
        return this.z;
    }

    public void setT(double t) {
        this.t = t;
    }

    public double getT() {
        return this.t;
    }

    public void setCluster(int n) {
        this.cluster_number = n;
    }

    public int getCluster() {
        return this.cluster_number;
    }

    //Calculates the distance between two points.
    protected static double distance(Point p, Point centroid) {
        return Math.sqrt(Math.pow((centroid.getY() - p.getY()), 2) + Math.pow((centroid.getX() - p.getX()), 2) + Math.pow((centroid.getZ() - p.getZ()), 2) + Math.pow((centroid.getT() - p.getT()), 2));
    }

    //Creates random point
    protected static Point createRandomPoint(int min, int max) {
    	Random r = new Random();
    	double x = min + (max - min) * r.nextDouble();
    	double y = min + (max - min) * r.nextDouble();
      double z = min + (max - min) * r.nextDouble();
    	double t = min + (max - min) * r.nextDouble();
    	return new Point(x,y,z,t);
    }

    //Creates points from given SensorData
    protected static List createPoints(List<SensorData> Data) {
      int numOfPoints=Data.size();
      List dataPoints = new ArrayList<Point>(numOfPoints);

      for(int i = 0; i < numOfPoints; i++) {
        SensorData aData= Data.get(i);
        Moments dataMoments=aData.getMoments();

        double x = dataMoments.getRM(0);
      	double y = dataMoments.getRM(1);
        double z = dataMoments.getRM(2);
      	double t = dataMoments.getRM(3);

        Point newPoint=new Point(x,y,z,t);
        dataPoints.add(newPoint);
    	}
    	return dataPoints;
    }

    protected static List createRandomPoints(int min, int max, int number) {
    	List points = new ArrayList(number);
    	for(int i = 0; i < number; i++) {
    		points.add(createRandomPoint(min,max));
    	}
    	return points;
    }

    public String toString() {
    	return "("+x+","+y+","+z+","+t+")";
    }
}

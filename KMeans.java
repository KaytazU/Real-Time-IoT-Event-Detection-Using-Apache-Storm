package udacity.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import udacity.storm.Point;

public class KMeans {

	//Number of Clusters. This metric should be related to the number of points
    private int NUM_CLUSTERS=3;
    //Number of Points
    private int NUM_POINTS= 15;
    //Min and Max for X,Y,Z,T coordinates
    private static final int MIN_COORDINATE = 0;
    private static final int MAX_COORDINATE = 10;

    private List<Point> points;
    private List<Cluster> clusters;
    private List<Point>  currentCentroids;
    private List<Point>  lastCentroids;

    public KMeans() {
    	this.points = new ArrayList<Point>();
    	this.clusters = new ArrayList<Cluster>();
    }

    public KMeans(int NUM_CLUSTERS, int NUM_POINTS) {
    	this.points = new ArrayList<Point>();
    	this.clusters = new ArrayList<Cluster>();
      this.NUM_CLUSTERS=NUM_CLUSTERS;
      this.NUM_POINTS=NUM_POINTS;
    }
    /*
    public static void main(String[] args) {

    	KMeans kmeans = new KMeans();
    	kmeans.init();
    	kmeans.calculate();
    }
    */
    //Initializes the process
    public void init(List<SensorData> centroids,List<SensorData> clusterData) {
    	//Create Points from given sensor data
      points=Point.createPoints(clusterData);

    	//Create Clusters
    	//Set Centroids
    	for (int i = 0; i < NUM_CLUSTERS; i++) {
    		Cluster cluster = new Cluster(i);
    		//Point centroid = Point.createRandomPoint(MIN_COORDINATE,MAX_COORDINATE);
        SensorData centroidData=centroids.get(i);
        Moments centroidMoments=centroidData.getMoments();
        Point centroidPoint=new Point(centroidMoments.getRM(0),centroidMoments.getRM(1),centroidMoments.getRM(2),centroidMoments.getRM(3));
    		cluster.setCentroid(centroidPoint);
    		clusters.add(cluster);
    	}
    	//Print Initial state
    	//plotClusters();
    }

	private void plotClusters() {
    	for (int i = 0; i < NUM_CLUSTERS; i++) {
    		Cluster c = (Cluster)clusters.get(i);
    		c.plotCluster();
    	}
    }

	//The process to calculate the K Means, with iterating method.

    public List calculate() {
        boolean finish = false;
        int iteration = 0;

        // Add in new data, one at a time, recalculating centroids with each new one.
        while(!finish) {
        	//Clear cluster state
        	clearClusters();
          //getting cluster centroids
        	lastCentroids = getCentroids();

        	//Assign points to the closer cluster
        	assignCluster();

          //Calculate new centroids.
        	calculateCentroids();

        	iteration++;

        	currentCentroids = getCentroids();

        	//Calculates total distance between new and old Centroids
        	double distance = 0;
        	for(int i = 0; i < lastCentroids.size(); i++) {
        		distance += Point.distance((Point)lastCentroids.get(i),(Point)currentCentroids.get(i));
        	}
        	//System.out.println("#################");
        	//System.out.println("Iteration: " + iteration);
        	//System.out.println("Centroid distances: " + distance);
        	//plotClusters();

        	if(distance == 0) {
        		finish = true;
            //System.out.println("CLUSTERING HAS FINISHED $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
            //printing clusters only when clustering is finished
            //for (int i = 0; i < NUM_CLUSTERS; i++) {
          	//	Cluster c = (Cluster)clusters.get(i);
          	//	c.plotCluster();
        	   //}
             return clusters;
          }
        }
        return null;

      }


    private void clearClusters() {
    	for(Cluster cluster :  clusters) {
    		cluster.clear();
    	}
    }

    private List getCentroids() {
    	List centroids = new ArrayList(NUM_CLUSTERS);
    	for(Cluster cluster : clusters) {
    		Point aux = cluster.getCentroid();
    		Point point = new Point(aux.getX(),aux.getY(),aux.getZ(),aux.getT());
    		centroids.add(point);
    	}
    	return centroids;
    }

    private void assignCluster() {
        double max = Double.MAX_VALUE;
        double min = max;
        int cluster = 0;
        double distance = 0.0;

        for(Point point : points) {
        	min = max;
            for(int i = 0; i < NUM_CLUSTERS; i++) {
            	Cluster c = clusters.get(i);
                distance = Point.distance(point, c.getCentroid());
                if(distance < min){
                    min = distance;
                    cluster = i;
                }
            }
            point.setCluster(cluster);
            clusters.get(cluster).addPoint(point);
        }
    }

    private void calculateCentroids() {
        for(Cluster cluster : clusters) {
            double sumX = 0;
            double sumY = 0;
            double sumZ = 0;
            double sumT = 0;
            List<Point> list = cluster.getPoints();
            int n_points = list.size();

            for(Point point : list) {
            	sumX += point.getX();
              sumY += point.getY();
              sumZ += point.getZ();
              sumT += point.getT();
            }

            Point centroid = cluster.getCentroid();
            if(n_points > 0) {
            	double newX = sumX / n_points;
            	double newY = sumY / n_points;
              double newZ = sumZ / n_points;
            	double newT = sumT / n_points;
                centroid.setX(newX);
                centroid.setY(newY);
                centroid.setZ(newZ);
                centroid.setT(newT);
            }
        }
    }
}

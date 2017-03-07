/*
Event detection topology for IoT data stream

Created by Umuralp Kaytaz


Topology consists of
Sensor data spouts
Key grouped hashing bolts
Local distance calculation bolts
Global distance calculation bolts
KMeans Clustering bolts
Event Partitioning Bolt 3
Drift detection bolts 5

==> Num of workers = 3+4+3+2+3+5 = 20
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

import udacity.storm.spout.*;

class EventDetectionTopology
{
  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    // Sensor data spout with a parallelism of 10
    builder.setSpout("Sensor-Data-Spout", new SensorDataSpout(), 10);

    // Key grouped hashing bolts
    builder.setBolt("Key-Grouped-Hashing-Bolt1",new KeyGroupedHashingBolt1(),1).shuffleGrouping("Sensor-Data-Spout");
    builder.setBolt("Key-Grouped-Hashing-Bolt2",new KeyGroupedHashingBolt1(),1).shuffleGrouping("Sensor-Data-Spout");
    builder.setBolt("Key-Grouped-Hashing-Bolt3",new KeyGroupedHashingBolt1(),1).shuffleGrouping("Sensor-Data-Spout");

    // Local distance calculation bolts
    builder.setBolt("Local-Dist-Calc-Bolt1",new LocalDistCalcBolt(),1).fieldsGrouping("Key-Grouped-Hashing-Bolt1",new Fields("emitted-key")).fieldsGrouping("Key-Grouped-Hashing-Bolt2",new Fields("emitted-key")).fieldsGrouping("Key-Grouped-Hashing-Bolt3",new Fields("emitted-key"));
    builder.setBolt("Local-Dist-Calc-Bolt2",new LocalDistCalcBolt(),1).fieldsGrouping("Key-Grouped-Hashing-Bolt1",new Fields("emitted-key")).fieldsGrouping("Key-Grouped-Hashing-Bolt2",new Fields("emitted-key")).fieldsGrouping("Key-Grouped-Hashing-Bolt3",new Fields("emitted-key"));
    builder.setBolt("Local-Dist-Calc-Bolt3",new LocalDistCalcBolt(),1).fieldsGrouping("Key-Grouped-Hashing-Bolt1",new Fields("emitted-key")).fieldsGrouping("Key-Grouped-Hashing-Bolt2",new Fields("emitted-key")).fieldsGrouping("Key-Grouped-Hashing-Bolt3",new Fields("emitted-key"));
    builder.setBolt("Local-Dist-Calc-Bolt4",new LocalDistCalcBolt(),1).fieldsGrouping("Key-Grouped-Hashing-Bolt1",new Fields("emitted-key")).fieldsGrouping("Key-Grouped-Hashing-Bolt2",new Fields("emitted-key")).fieldsGrouping("Key-Grouped-Hashing-Bolt3",new Fields("emitted-key"));

    // Global distance calculation bolts
    builder.setBolt("Global-Dist-Calc-Bolt1",new GlobalDistCalcBolt(),1).shuffleGrouping("Local-Dist-Calc-Bolt1").shuffleGrouping("Local-Dist-Calc-Bolt2").shuffleGrouping("Local-Dist-Calc-Bolt3").shuffleGrouping("Local-Dist-Calc-Bolt4");
    builder.setBolt("Global-Dist-Calc-Bolt2",new GlobalDistCalcBolt(),1).shuffleGrouping("Local-Dist-Calc-Bolt1").shuffleGrouping("Local-Dist-Calc-Bolt2").shuffleGrouping("Local-Dist-Calc-Bolt3").shuffleGrouping("Local-Dist-Calc-Bolt4");
    builder.setBolt("Global-Dist-Calc-Bolt3",new GlobalDistCalcBolt(),1).shuffleGrouping("Local-Dist-Calc-Bolt1").shuffleGrouping("Local-Dist-Calc-Bolt2").shuffleGrouping("Local-Dist-Calc-Bolt3").shuffleGrouping("Local-Dist-Calc-Bolt4");
    // Final clustering bolts
    builder.setBolt("Final-Clustering-Bolt1",new FinalClusteringBolt(),1).shuffleGrouping("Global-Dist-Calc-Bolt1").shuffleGrouping("Global-Dist-Calc-Bolt2").shuffleGrouping("Global-Dist-Calc-Bolt3");
    builder.setBolt("Final-Clustering-Bolt2",new FinalClusteringBolt(),1).shuffleGrouping("Global-Dist-Calc-Bolt1").shuffleGrouping("Global-Dist-Calc-Bolt2").shuffleGrouping("Global-Dist-Calc-Bolt3");

    //event partitioning bolts
    builder.setBolt("Event-Partitioning-Bolt1",new EventPartitioningBolt(),1).shuffleGrouping("Final-Clustering-Bolt1").shuffleGrouping("Final-Clustering-Bolt2");
    builder.setBolt("Event-Partitioning-Bolt2",new EventPartitioningBolt(),1).shuffleGrouping("Final-Clustering-Bolt1").shuffleGrouping("Final-Clustering-Bolt2");
    builder.setBolt("Event-Partitioning-Bolt3",new EventPartitioningBolt(),1).shuffleGrouping("Final-Clustering-Bolt1").shuffleGrouping("Final-Clustering-Bolt2");

    //Number of clusters= num of events= number of machines for drift detection
    builder.setBolt("Drift-Detection-Bolt1",new DriftDetectionBolt(),1).fieldsGrouping("Event-Partitioning-Bolt1",new Fields("emitted-event-index")).fieldsGrouping("Event-Partitioning-Bolt2",new Fields("emitted-event-index")).fieldsGrouping("Event-Partitioning-Bolt3",new Fields("emitted-event-index"));
    builder.setBolt("Drift-Detection-Bolt2",new DriftDetectionBolt(),1).fieldsGrouping("Event-Partitioning-Bolt1",new Fields("emitted-event-index")).fieldsGrouping("Event-Partitioning-Bolt2",new Fields("emitted-event-index")).fieldsGrouping("Event-Partitioning-Bolt3",new Fields("emitted-event-index"));
    builder.setBolt("Drift-Detection-Bolt3",new DriftDetectionBolt(),1).fieldsGrouping("Event-Partitioning-Bolt1",new Fields("emitted-event-index")).fieldsGrouping("Event-Partitioning-Bolt2",new Fields("emitted-event-index")).fieldsGrouping("Event-Partitioning-Bolt3",new Fields("emitted-event-index"));
    builder.setBolt("Drift-Detection-Bolt4",new DriftDetectionBolt(),1).fieldsGrouping("Event-Partitioning-Bolt1",new Fields("emitted-event-index")).fieldsGrouping("Event-Partitioning-Bolt2",new Fields("emitted-event-index")).fieldsGrouping("Event-Partitioning-Bolt3",new Fields("emitted-event-index"));
    builder.setBolt("Drift-Detection-Bolt5",new DriftDetectionBolt(),1).fieldsGrouping("Event-Partitioning-Bolt1",new Fields("emitted-event-index")).fieldsGrouping("Event-Partitioning-Bolt2",new Fields("emitted-event-index")).fieldsGrouping("Event-Partitioning-Bolt3",new Fields("emitted-event-index"));


    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(20);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(20);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("Event-Detection-Topology", conf, builder.createTopology());

      // let the topology run for 300 seconds (5 minutes). note topologies never terminate!
      Utils.sleep(300000);

      // now kill the topology
      cluster.killTopology("Event-Detection-Topology");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}

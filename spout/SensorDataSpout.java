/*
Sensor data spout for IoT event detection topology

Created by Umuralp Kaytaz

*/
package udacity.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.lang.*;
import java.util.Arrays;

import udacity.storm.SensorData;

public class SensorDataSpout extends BaseRichSpout{

//Defining the collector
SpoutOutputCollector _collector;
//For storing sensor data
List<SensorData> sensorOutput;

//Overriding inherited methods from IBolt interface
public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
  _collector = collector;
}

public void nextTuple() {
  //to adjust emit frequency (1/10 sec)
  Utils.sleep(100);
  //getting data
  sensorOutput=SensorData.generateMultipleData(10);

  _collector.emit(new Values(sensorOutput));

}

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  declarer.declare(new Fields("sensorOutput"));
  }

}

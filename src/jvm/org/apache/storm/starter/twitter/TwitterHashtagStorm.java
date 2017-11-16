package org.apache.storm.starter.twitter;

import java.util.*;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.starter.spout.RandomSentenceSpout;

import java.util.HashMap;
import java.util.Map;

public class TwitterHashtagStorm {
   public static void main(String[] args) throws Exception{
      String consumerKey = "FHud3D3NEgTYTdltAbHoqiY1N";
      String consumerSecret = "vMXBRjGrzznSJKtGTmraKutTWGERXITAe2avJVr8b3RElBsIsv";
		
      String accessToken = "86763765-i2VRTqwCPuijn6lX5YWGCihflfWnGlnRx69jlIM2b";
      String accessTokenSecret = "nEA9Lm854lMlI5vjWQI8lGEgBWDk4yTPLrUp46NTDlgGM";
		
      String[] arguments = args.clone();
      String[] keyWords = new String[] {"lol","VaiSafadao","ClaudiaLeitte","huehuebr","BOLSONARO2018"};
		
      Config config = new Config();
      config.setDebug(true);
		
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("twitter-spout", new TwitterSampleSpout(consumerKey,
         consumerSecret, accessToken, accessTokenSecret, keyWords));

      builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
         .shuffleGrouping("twitter-spout");

      builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
         .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));
			

      config.setNumWorkers(3);
      StormSubmitter.submitTopologyWithProgressBar("twitter", config, builder.createTopology());
   }
}

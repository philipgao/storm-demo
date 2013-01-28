/**
 * 
 */
package com.ssparrow.storm.demo.topo;

import com.ssparrow.storm.demo.bolt.slidingwindow.SlidingWindowCounterBolt;
import com.ssparrow.storm.demo.bolt.wordcount.LowercaseBolt;
import com.ssparrow.storm.demo.bolt.wordcount.WordCountBolt;
import com.ssparrow.storm.demo.bolt.wordcount.WordExtractorBolt;
import com.ssparrow.storm.demo.sprout.RandomSentenceSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author Gao, Fei
 *
 */
public class SlidingWindowWordCountTopology {
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException{
		TopologyBuilder topologyBuilder=new TopologyBuilder();
		topologyBuilder.setSpout("randomsentence", new RandomSentenceSpout());
		topologyBuilder.setBolt("wordextractor", new WordExtractorBolt(),5).shuffleGrouping("randomsentence");
		topologyBuilder.setBolt("lowercase", new LowercaseBolt(), 5).shuffleGrouping("wordextractor");
		topologyBuilder.setBolt("wordcount", new WordCountBolt(), 5).fieldsGrouping("lowercase", new Fields("word"));
		topologyBuilder.setBolt("slidingwindowcount", new SlidingWindowCounterBolt(5, 6), 5).fieldsGrouping("wordcount", new Fields("word"));
		
		Config config=new Config();
		config.setDebug(true);
		
		if(args.length>0){
			config.setNumWorkers(5);
			
			StormSubmitter.submitTopology("slidingwindowcount", config, topologyBuilder.createTopology());
		}else{
			config.setMaxTaskParallelism(5);
			
			LocalCluster localCluster=new LocalCluster();
			localCluster.submitTopology("slidingwindowcount", config, topologyBuilder.createTopology());
		}
	}
}

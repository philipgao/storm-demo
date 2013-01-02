/**
 * 
 */
package com.ssparrow.storm.demo.topo;

import com.ssparrow.storm.demo.bolt.wordcount.LowercaseBolt;
import com.ssparrow.storm.demo.bolt.wordcount.WordCountBolt;
import com.ssparrow.storm.demo.bolt.wordcount.WordExtractorBolt;
import com.ssparrow.storm.demo.sprout.LineReadSprout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author Gao, Fei
 *
 */
public class WordCountTopology {
	
	public static void main(String [] args) throws Exception{
		TopologyBuilder builder=new TopologyBuilder();
		
		builder.setSpout("sprout", new LineReadSprout());
		
		builder.setBolt("extract", new WordExtractorBolt(), 5).shuffleGrouping("sprout");

		builder.setBolt("lower", new LowercaseBolt(), 10).shuffleGrouping("extract");
		
		builder.setBolt("count", new WordCountBolt(), 20).fieldsGrouping("lower", new Fields("word"));
		
		Config conf=new Config();
		conf.setDebug(true);
		
		if(args!=null &&args.length>0){
			conf.setNumWorkers(3);
			
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}else{
			conf.setMaxTaskParallelism(5);
			
			LocalCluster localCluster=new LocalCluster();
			localCluster.submitTopology("word-count", conf, builder.createTopology());
			
			Thread.sleep(10*1000);
			
			localCluster.shutdown();
		}
	}
}

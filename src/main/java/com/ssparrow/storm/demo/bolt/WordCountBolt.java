/**
 * 
 */
package com.ssparrow.storm.demo.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Gao, Fei
 *
 */
public class WordCountBolt extends BaseRichBolt {
	OutputCollector collector;
	Map<String, Integer> wordCountMap=new HashMap<String, Integer>();

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		this.collector=collector;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String word=input.getString(0);
		
		int count=0;
		if(wordCountMap.get(word)!=null){
			count=wordCountMap.get(word).intValue();
		}
		count+=1;
		wordCountMap.put(word, count);
		
		collector.emit(new Values(word,count));
		collector.ack(input);
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}

}

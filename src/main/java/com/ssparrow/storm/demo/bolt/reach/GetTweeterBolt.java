/**
 * 
 */
package com.ssparrow.storm.demo.bolt.reach;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Gao, Fei
 *
 */
public class GetTweeterBolt extends BaseBasicBolt {
	private OutputCollector collector;
	
	private Map<String, List<String>> urlTweeterMap=new HashMap<String, List<String>>(){{
		put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan")); 
	    put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan")); 
	    put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john")); 
	}};

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
	 */
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Object id = input.getValue(0);
		String url=input.getString(1);
		
		List<String> tweeterList = urlTweeterMap.get(url);
		if(tweeterList!=null){
			for(String tweeter:tweeterList){
				collector.emit(new Values(id, tweeter));
			}
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "tweeter"));
	}

}

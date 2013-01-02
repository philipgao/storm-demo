/**
 * 
 */
package com.ssparrow.storm.demo.bolt.wordcount;

import java.util.Map;
import java.util.StringTokenizer;

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
public class WordExtractorBolt extends BaseRichBolt {
	OutputCollector collector;

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
		String line = input.getString(0);
		
		if(line!=null){
			StringTokenizer st = new StringTokenizer(line," ,.;");
			while(st.hasMoreTokens()){
				String word = st.nextToken();
				collector.emit(new Values(word));
			}
		}

		collector.ack(input);
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}

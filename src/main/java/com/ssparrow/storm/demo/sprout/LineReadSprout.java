/**
 * 
 */
package com.ssparrow.storm.demo.sprout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author Gao, Fei
 *
 */
public class LineReadSprout extends BaseRichSpout {
	SpoutOutputCollector collector;
	BufferedReader reader;

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
	 */
	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
		this.collector=collector;
		
		try {
			reader=new BufferedReader(new FileReader("src/main/resources/doi.txt"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		String line="";
		
		if(reader!=null){
			try {
				line=reader.readLine();
				if(line==null){
					return;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		collector.emit(new Values(line));
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}

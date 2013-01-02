/**
 * 
 */
package com.ssparrow.storm.demo.bolt.reach;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Gao, Fei
 *
 */
public class PartialUniqueCounterBolt extends BaseBatchBolt {
	private BatchOutputCollector collector;
	private Object id;
	
	private Set<String> followerSet=new HashSet<String>();

	/* (non-Javadoc)
	 * @see backtype.storm.coordination.IBatchBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.coordination.BatchOutputCollector, java.lang.Object)
	 */
	@Override
	public void prepare(Map conf, TopologyContext context,BatchOutputCollector collector, Object id) {
		this.collector=collector;
		this.id=id;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.coordination.IBatchBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple tuple) {
		followerSet.add(tuple.getString(1));
	}

	/* (non-Javadoc)
	 * @see backtype.storm.coordination.IBatchBolt#finishBatch()
	 */
	@Override
	public void finishBatch() {
		collector.emit(new Values(id, followerSet.size()));
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "partial-count"));
	}

}

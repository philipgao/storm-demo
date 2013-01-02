/**
 * 
 */
package com.ssparrow.storm.demo.bolt.reach;

import java.util.Map;

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
public class CountAggregateBolt extends BaseBatchBolt {
	private BatchOutputCollector collector;
	private Object id;
	
	private int reach=0;
	
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
		reach+=tuple.getInteger(1);
	}

	/* (non-Javadoc)
	 * @see backtype.storm.coordination.IBatchBolt#finishBatch()
	 */
	@Override
	public void finishBatch() {
		collector.emit(new Values(id, reach));
		
		System.out.println("!!!!!ID:"+id+", Reach:"+reach);
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "reach"));
	}

}

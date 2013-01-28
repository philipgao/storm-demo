/**
 * 
 */
package com.ssparrow.storm.demo.bolt.slidingwindow;

import java.util.HashMap;
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
public class SlidingWindowCounterBolt extends BaseRichBolt{
	private OutputCollector collector;
	
	private int bucketNum=5;
	private int bucketSize=60;
	
	private long startTime;
	private boolean isStarting=true;
	private int bucketBeginIndex;
	
	private Map<Object, Integer[]> counterMap=new HashMap<Object, Integer[]>();
	/**
	 * @param bucketNum
	 * @param bucketSize
	 */
	public SlidingWindowCounterBolt(int bucketNum, int bucketSize) {
		this.bucketNum = bucketNum;
		this.bucketSize = bucketSize;
	}
	
	public void increaseObjectCount(Object object, int count){
		int index=getBucketIndex(System.currentTimeMillis());
		
		synchronized(counterMap){
			Integer[] counts = counterMap.get(object);
			if(counts==null){
				counts=new Integer[bucketNum];
				counterMap.put(object, counts);
			}
			if(counts[index]==null){
				counts[index]=count;
			}else{
				counts[index]+=count;
			}
		}
	}
	
	public int getBucketIndex(long milliSecond){
		return (int) ((milliSecond/1000)%bucketNum);
	}
	
	public boolean isStarting(){
		if(isStarting && (System.currentTimeMillis()-startTime)/1000>=bucketNum*bucketSize){
			isStarting=false;
		}
		return isStarting;
	}
	
	public int getTotalCount(Integer[] counts){
		int sum=0;
		for(Integer count:counts){
			sum+=count==null?0:count.intValue();
		}
		return sum;
	}
	
	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timestamp", "object", "count"));
	}

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.task.OutputCollector)
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector _collector) {
		this.collector=_collector;

		startTime=System.currentTimeMillis();
		bucketBeginIndex=getBucketIndex(startTime);
		
		Thread bucketCleanerThread=new Thread(new Runnable(){

			@Override
			public void run() {
				while(true){
					try {
						int bucketIndex=getBucketIndex(System.currentTimeMillis());
						
						if(bucketIndex!=bucketBeginIndex){
							synchronized(counterMap){
								for(Object object:counterMap.keySet()){
									Integer[] counts=counterMap.get(object);
									
									collector.emit(new Values(System.currentTimeMillis(), object.toString(), getTotalCount(counts)));
									System.out.println("!!!!!!!!!!!!!!!Total "+ object.toString()+" "+ getTotalCount(counts));
									
									if(!isStarting()){
										counts[bucketIndex]=0;
									}
								}
							}
						}
						
						Thread.sleep((bucketSize-(System.currentTimeMillis()/1000)%bucketSize)*1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			
		});
		
		bucketCleanerThread.start();
	}

	/* (non-Javadoc)
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 */
	@Override
	public void execute(Tuple input) {
		String word=input.getString(0);
		int count=input.getInteger(1);
		
		System.out.println("!!!!!!!!!!!!!!!Increase "+ word+" "+ count);
		this.increaseObjectCount(word, count);
	}
}

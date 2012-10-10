/**
 * 
 */
package com.ssparrow.storm.demo.trident;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author Gao, Fei
 *
 */
public class TridentWordCount {

	/**
	 * @param args
	 * @throws Exception 
	 * @throws AlreadyAliveException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, Exception {
		Config config=new Config();
		config.setMaxSpoutPending(20);
		
		if(args.length==0){
			config.setMaxTaskParallelism(5);
			
			LocalCluster localCluster=new LocalCluster();
			LocalDRPC localDRPC=new LocalDRPC();

			StormTopology stormTopology = buildTopology(localDRPC);
			
			localCluster.submitTopology("wordcount", config, stormTopology);
			
			for(int i=0;i<100;i++){
				System.out.println("DRPC RESULT:"+localDRPC.execute("words", "cat the dog jumped"));
				Thread.sleep(1000);
			}
		}else{
			config.setNumWorkers(3);

			StormTopology stormTopology = buildTopology(null);
			
			StormSubmitter.submitTopology(args[0], config, stormTopology);
		}
	}
	
	public static StormTopology buildTopology(LocalDRPC localDRPC){
		FixedBatchSpout spout=new FixedBatchSpout(new Fields("sentence"), 3, 
				new Values("The cow jumped over the moon"),
                new Values("The man went to the store and bought some candy"),
                new Values("Four score and seven years ago"),
                new Values("How many apples can you eat"),
                new Values("To be or not to be the person"));
		spout.setCycle(true);
		
		TridentTopology topology=new TridentTopology();
		TridentState tridentState = topology.newStream("spout1", spout)
			.parallelismHint(16)
			.each(new Fields("sentence"), new Split(), new Fields("item"))
			.each(new Fields("item"), new LowerCase(), new Fields("word"))
			.groupBy(new Fields("word"))
			.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
			.parallelismHint(6);
		
		topology.newDRPCStream("words", localDRPC)
			.each(new Fields("args"), new Split(), new Fields("word"))
			.groupBy(new Fields("word"))
			.stateQuery(tridentState, new Fields("word"), new MapGet(), new Fields("count"))
			.each(new Fields("count"), new FilterNull())
			.aggregate(new Fields("count"), new Sum(), new Fields("sum"));
		
		return topology.build();
	}
}


class Split extends BaseFunction{

	/* (non-Javadoc)
	 * @see storm.trident.operation.Function#execute(storm.trident.tuple.TridentTuple, storm.trident.operation.TridentCollector)
	 */
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sentence=tuple.getString(0);
		for(String word:sentence.split(" ")){
			collector.emit(new Values(word));
		}
	}
	
}

class LowerCase extends BaseFunction{

	/* (non-Javadoc)
	 * @see storm.trident.operation.Function#execute(storm.trident.tuple.TridentTuple, storm.trident.operation.TridentCollector)
	 */
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String word=tuple.getString(0);
		String lowerCase = word.toLowerCase();
		collector.emit(new Values(lowerCase));
	}
	
}
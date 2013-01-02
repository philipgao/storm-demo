/**
 * 
 */
package com.ssparrow.storm.demo.trident;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.ReadOnlyState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author Gao, Fei
 *
 */
public class TridentReach {
	public static Map<String, List<String>> urlTweeterMap=new HashMap<String, List<String>>(){{
		put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan")); 
	    put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan")); 
	    put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john")); 
	}};
	
	public static Map<String, List<String>> tweeterFollowerMap=new HashMap<String, List<String>>(){{
        put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
        put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
        put("tim", Arrays.asList("alex"));
        put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
        put("adam", Arrays.asList("david", "carissa"));
        put("mike", Arrays.asList("john", "bob"));
        put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
	}};

	/**
	 * @param args
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
		Config config=new Config();
		config.setDebug(true);
		
		if(args!=null && args.length>0){
			config.setNumWorkers(4);
			
			StormTopology topology = buildTopology(null);
			StormSubmitter.submitTopology(args[0], config, topology);
		}else{
			config.setMaxTaskParallelism(4);
			
			LocalDRPC localDRPC=new LocalDRPC();
			
			StormTopology topology = buildTopology(localDRPC);
			
			LocalCluster localCluster=new LocalCluster();
			localCluster.submitTopology("reach-drpc", config, topology);
			
			Thread.sleep(2000);
			
			String[] urls = new String[] { "foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com"};
            for(String url: urls) {
                System.out.println("Reach of " + url + ": " + localDRPC.execute("reach", url));
            }
            
            localCluster.shutdown();
            localDRPC.shutdown();
		}
	}

	public static StormTopology buildTopology(LocalDRPC drpc){
		TridentTopology tridentTopology=new TridentTopology();
		
		TridentState urlTweeterState = tridentTopology.newStaticState(new SingleKeyMapState.Factory(urlTweeterMap));
		TridentState tweeterFollowerState = tridentTopology.newStaticState(new SingleKeyMapState.Factory(tweeterFollowerMap));
		
		tridentTopology.newDRPCStream("reach", drpc)
			.stateQuery(urlTweeterState, new Fields("args"), new MapGet(), new Fields("tweeters"))
			.each(new Fields("tweeters"), new ExpandList(), new Fields("tweeter"))
			.groupBy(new Fields("tweeter"))
			.stateQuery(tweeterFollowerState, new Fields("tweeter"), new MapGet(), new Fields("followers"))
			.each(new Fields("followers"), new ExpandList(), new Fields("follower"))
			.groupBy(new Fields("follower"))
			.aggregate(new One(), new Fields("one"))
			.aggregate(new Fields("one"), new Sum(), new Fields("reach"));
		
		return tridentTopology.build();
	}
	
	public static  class ExpandList extends BaseFunction{

		/* (non-Javadoc)
		 * @see storm.trident.operation.Function#execute(storm.trident.tuple.TridentTuple, storm.trident.operation.TridentCollector)
		 */
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			List list = (List)tuple.getValue(0);
			if(list!=null){
				for(Object object:list){
					collector.emit(new Values(object));
				}
			}
		}
		
	}
	
	public static class One implements CombinerAggregator<Integer>{

		/* (non-Javadoc)
		 * @see storm.trident.operation.CombinerAggregator#init(storm.trident.tuple.TridentTuple)
		 */
		@Override
		public Integer init(TridentTuple tuple) {
			return 1;
		}

		/* (non-Javadoc)
		 * @see storm.trident.operation.CombinerAggregator#combine(java.lang.Object, java.lang.Object)
		 */
		@Override
		public Integer combine(Integer val1, Integer val2) {
			return 1;
		}

		/* (non-Javadoc)
		 * @see storm.trident.operation.CombinerAggregator#zero()
		 */
		@Override
		public Integer zero() {
			return 1;
		}
		
	}
	
	public static class SingleKeyMapState extends ReadOnlyState implements ReadOnlyMapState<Object>{
		private Map<String, List<String>> map;
		
		/**
		 * @param map
		 */
		public SingleKeyMapState(Map<String, List<String>> map) {
			super();
			this.map = map;
		}


		/* (non-Javadoc)
		 * @see storm.trident.state.map.ReadOnlyMapState#multiGet(java.util.List)
		 */
		@Override
		public List<Object> multiGet(List<List<Object>> keys) {
			List<Object> result=new ArrayList<Object>();
			
			for(List<Object> key:keys){
				Object singleKey=key.get(0);
				result.add(map.get(singleKey));
			}
			
			return result;
		}
		
		public static class Factory implements StateFactory{
			private Map<String, List<String>> map;
			
			
			/**
			 * @param map
			 */
			public Factory(Map<String, List<String>> map) {
				super();
				this.map = map;
			}


			/* (non-Javadoc)
			 * @see storm.trident.state.StateFactory#makeState(java.util.Map, int, int)
			 */
			@Override
			public State makeState(Map conf, int partitionIndex,int numPartitions) {
				return new SingleKeyMapState(map);
			}
			
		}
		
	}
}

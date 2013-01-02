/**
 * 
 */
package com.ssparrow.storm.demo.topo;

import com.ssparrow.storm.demo.bolt.reach.CountAggregateBolt;
import com.ssparrow.storm.demo.bolt.reach.GetFollowerBolt;
import com.ssparrow.storm.demo.bolt.reach.GetTweeterBolt;
import com.ssparrow.storm.demo.bolt.reach.PartialUniqueCounterBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;

/**
 * @author Gao, Fei
 *
 */
public class ReachTopology {

	/**
	 * @param args
	 * @throws InvalidTopologyException 
	 * @throws AlreadyAliveException 
	 */
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		LinearDRPCTopologyBuilder topologyBuilder=new LinearDRPCTopologyBuilder("reach");
		
		topologyBuilder.addBolt(new GetTweeterBolt(), 4);
		topologyBuilder.addBolt(new GetFollowerBolt(), 12).shuffleGrouping();
		topologyBuilder.addBolt(new PartialUniqueCounterBolt(), 6).fieldsGrouping(new Fields("id","follower"));
		topologyBuilder.addBolt(new CountAggregateBolt(),3).fieldsGrouping(new Fields("id"));
		
		Config config=new Config();
		config.setDebug(true);
		
		if(args!=null && args.length>0){
			config.setNumWorkers(4);
			StormSubmitter.submitTopology(args[0], config, topologyBuilder.createRemoteTopology());
		}else{
			config.setMaxTaskParallelism(4);
			
			LocalDRPC locaDrpc=new LocalDRPC();
			
			LocalCluster localCluster=new LocalCluster();
			localCluster.submitTopology("reach-drpc", config, topologyBuilder.createLocalTopology(locaDrpc));
			
			String[] urls = new String[] { "foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com"};
            for(String url: urls) {
                System.out.println("Reach of " + url + ": " + locaDrpc.execute("reach", url));
            }
            
            localCluster.shutdown();
            locaDrpc.shutdown();
		}
	}

}

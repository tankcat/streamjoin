package main;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;


import bolt.equijoin.DBolt;
import bolt.equijoin.UBolt;
import bolt.thetajoin.ControllerLBBoltForJoin;
import spout.loadbalance.LBSpoutForJoin;
import tool.SystemParameters;
import org.apache.storm.utils.Utils;

public class LoadBalanceTopology {
	public static void main(String[] args){
		TopologyBuilder builder=new TopologyBuilder();
		//builder.setSpout(SystemParameters.keySpoutId, new WordLBSpout(SystemParameters.host,SystemParameters.port,SystemParameters.dataSource),SystemParameters.keySpoutPara);
		builder.setSpout(SystemParameters.keySpoutId,new LBSpoutForJoin(SystemParameters.host,SystemParameters.port,"lineitem"),SystemParameters.keySpoutPara);
		builder.setBolt(SystemParameters.uBoltId, new UBolt(SystemParameters.host,SystemParameters.port),SystemParameters.uBoltPara)
				.shuffleGrouping(SystemParameters.keySpoutId)
				.directGrouping(SystemParameters.keySpoutId, "sendend")
				.directGrouping(SystemParameters.ctrlBoltId);
		builder.setBolt(SystemParameters.dBoltId, new DBolt(SystemParameters.host,SystemParameters.port,"keyloads"),SystemParameters.dBoltPara)
				.directGrouping(SystemParameters.uBoltId)
				.directGrouping(SystemParameters.ctrlBoltId)
				.directGrouping(SystemParameters.dBoltId);
		builder.setBolt(SystemParameters.ctrlBoltId,new ControllerLBBoltForJoin(SystemParameters.host,SystemParameters.port,"keyloads"),SystemParameters.ctrlBoltPara)
				.directGrouping(SystemParameters.dBoltId);
		
		Config conf=new Config();
		conf.setDebug(false);
		conf.setMaxSpoutPending(500);
		conf.setNumWorkers(25);
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, SystemParameters.emitFrequencyInSeconds);
		
		if(args.length==0){
			LocalCluster cluster=new LocalCluster();
			cluster.submitTopology(SystemParameters.ldTopologyId, conf, builder.createTopology());
			Utils.sleep(1000000);
			cluster.killTopology(SystemParameters.ldTopologyId);
			cluster.shutdown();
			
		}else{
			try {
				System.out.println("================================");
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} 
	}
}

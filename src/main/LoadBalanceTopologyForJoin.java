package main;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import bolt.thetajoin.ControllerLBBoltForJoin;
import bolt.thetajoin.DBoltForJoin;
import bolt.thetajoin.UBoltForJoin;
import spout.loadbalance.LBSpoutForJoin;
import tool.SystemParameters;
import org.apache.storm.utils.Utils;

public class LoadBalanceTopologyForJoin {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TopologyBuilder builder=new TopologyBuilder();
		builder.setSpout(SystemParameters.keySpoutId,new LBSpoutForJoin(SystemParameters.host,SystemParameters.port,"lineitem"),5);
		builder.setBolt(SystemParameters.uBoltId,new UBoltForJoin(SystemParameters.host,SystemParameters.port,SystemParameters.range),SystemParameters.uBoltPara)
				.shuffleGrouping(SystemParameters.keySpoutId)
				.directGrouping(SystemParameters.keySpoutId, "sendend")
				.directGrouping(SystemParameters.ctrlBoltId);
		builder.setBolt(SystemParameters.dBoltId,new DBoltForJoin(SystemParameters.host,SystemParameters.port,SystemParameters.range,"keyloads"),SystemParameters.dBoltPara)
				.directGrouping(SystemParameters.uBoltId)
				.directGrouping(SystemParameters.ctrlBoltId)
				.directGrouping(SystemParameters.dBoltId);
		builder.setBolt(SystemParameters.ctrlBoltId,new ControllerLBBoltForJoin(SystemParameters.host,SystemParameters.port,"keyloads"),SystemParameters.ctrlBoltPara)
				.directGrouping(SystemParameters.dBoltId);
		
		Config conf=new Config();
		conf.setDebug(false);
		conf.setMaxSpoutPending(100);
		conf.setNumWorkers(10);
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

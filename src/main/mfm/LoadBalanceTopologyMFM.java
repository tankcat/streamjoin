package main.mfm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import bolt.mfm.ControllerMFM;
import bolt.mfm.JoinMFM;
import spout.mfm.SpoutMFM;
import tool.SystemParameters;
import util.mfm.Systemparameters;

public class LoadBalanceTopologyMFM {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TopologyBuilder builder=new TopologyBuilder();
		String lineitemID="orders";
		int[] initialMatrix={1,1};
		String controllerID="controller";
		String joinerID="order-lineitem-bolt";
		
		builder.setSpout(lineitemID, new SpoutMFM(SystemParameters.host,SystemParameters.port,"orders",initialMatrix,joinerID),1);
		
		builder.setBolt(controllerID, new ControllerMFM(SystemParameters.host,SystemParameters.port,15,joinerID,1)).directGrouping(joinerID, Systemparameters.ThetaJoinerSignal);
		builder.setBolt(joinerID,new JoinMFM(SystemParameters.host,SystemParameters.port,controllerID,joinerID),40)
											   .directGrouping(lineitemID,Systemparameters.ThetaControllerSignal)
											   .directGrouping(lineitemID, Systemparameters.DATA_STREAM)
											   .directGrouping(controllerID,Systemparameters.ThetaControllerSignal)
											   .directGrouping(joinerID,Systemparameters.ThetaDataMigration)
											   .directGrouping(joinerID,Systemparameters.ThetaDataSendEOF);
	
		
		Config conf=new Config();
		conf.setDebug(false);
		conf.setMaxSpoutPending(500);
		conf.setMessageTimeoutSecs(60000);
		conf.setNumWorkers(30);
		if(args.length==0){
			LocalCluster cluster=new LocalCluster();
			cluster.submitTopology("tolologymfm", conf, builder.createTopology());
			Utils.sleep(1000000);
			cluster.killTopology("tolologymfm");
			cluster.shutdown();
		}else{
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for(int i=0;i<10;i++){
				System.out.println("==================================================================================");
			}
		}
	}

	

}

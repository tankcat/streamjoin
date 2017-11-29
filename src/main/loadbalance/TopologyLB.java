package main.loadbalance;



import bolt.loadbalance.*;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import spout.loadbalance.LBSpout;
import util.SystemParameters;

/**
 * Created by stream on 16-11-2.
 */
public class TopologyLB {
    public static void main(String[] args){
        String choose="last";
        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout(SystemParameters.LBSPOUT,new LBSpout("key_100w_0_5"),SystemParameters.SPOUT_PARA);
        builder.setBolt(SystemParameters.UBOLT,new UBolt("="),SystemParameters.UDBOLT_PARA)
                .shuffleGrouping(SystemParameters.LBSPOUT)
                .directGrouping(SystemParameters.LBCONTROLLER);
        builder.setBolt(SystemParameters.DBOLT,new DBolt(choose),SystemParameters.DBOLT_PARA)
                .directGrouping(SystemParameters.UBOLT)
                .directGrouping(SystemParameters.LBCONTROLLER)
                .directGrouping(SystemParameters.DBOLT);
        switch (choose){
            case "last": builder.setBolt(SystemParameters.LBCONTROLLER,new LastSplitCtrllerBolt(),SystemParameters.CTRL_PARA)
                    .directGrouping(SystemParameters.DBOLT);break;
            case "first": builder.setBolt(SystemParameters.LBCONTROLLER,new FirstSplitCtrllerBolt(),SystemParameters.CTRL_PARA)
                    .directGrouping(SystemParameters.DBOLT);break;
            case "random": builder.setBolt(SystemParameters.LBCONTROLLER,new RandomSplitCtrllerBolt(),SystemParameters.CTRL_PARA)
                    .directGrouping(SystemParameters.DBOLT);break;
        }
        Config conf=new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(SystemParameters.SPOUT_MAX_PEND);
        conf.setMessageTimeoutSecs(6000);
        conf.setNumWorkers(4);
        //本地模式
        if(args.length==0){
            LocalCluster cluster=new LocalCluster();
            cluster.submitTopology(SystemParameters.LBTOPO,conf,builder.createTopology());
            Utils.sleep(SystemParameters.SLEEP);
            cluster.killTopology(SystemParameters.LBTOPO);
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
            } catch (org.apache.storm.generated.AuthorizationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println("==================================================================================");
        }
    }
}

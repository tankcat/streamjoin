package bolt.loadbalance;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import tool.FileProcess;
import tool.loadbalance.PlanGeneratorSplitRandom;
import tool.SerializeUtil;
import util.SystemParameters;
import util.loadbalance.MigrationItem;

import java.io.BufferedWriter;
import java.util.*;

/**
 * Created by stream on 16-9-26.
 */
public class RandomSplitCtrllerBolt extends BaseRichBolt{
    private OutputCollector _collector;
    private transient Jedis jedis;
    private List<Integer> dBoltPhysicalIDs;
    private List<Integer> uBoltPhysicalIDs;
    private HashSet<Integer> usedDBolt;
    private int usedDBoltNum;
    private int curEndNum=0;
    private boolean flag=false;
    private int reportTime=1;
    private BufferedWriter bwTable;
    private BufferedWriter bwLoad;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector=outputCollector;
        jedis=getConnectJedis();
        dBoltPhysicalIDs=topologyContext.getComponentTasks(SystemParameters.DBOLT);
        uBoltPhysicalIDs=topologyContext.getComponentTasks(SystemParameters.UBOLT);
        usedDBolt=new HashSet<>();
        usedDBoltNum=SystemParameters.DBOLT_PARA_INIT;
        for(int i=0;i<usedDBoltNum;i++){
            usedDBolt.add(i);
        }
        jedis.set("usedDBolt".getBytes(),SerializeUtil.serialize(usedDBolt));
        bwTable =FileProcess.getWriter("Experiment/tableRandom.txt");
        bwLoad=FileProcess.getWriter("Experiment/loadRandom.txt");
    }

    @Override
    public void execute(Tuple tuple) {
        String signal=tuple.getStringByField(SystemParameters.SIGNAL);
        if(signal.equalsIgnoreCase(SystemParameters.JOINER_MIG_END)){
            curEndNum++;
            System.out.println("收到"+dBoltPhysicalIDs.indexOf(tuple.getSourceTask())+"号DBolt迁移结束信号");
            if(curEndNum==PlanGeneratorSplitRandom.endCheckInfo.size()){
                System.out.println("收到所有DBolt迁移结束信号\n\n");
                curEndNum=0;
                PlanGeneratorSplitRandom.reset();
                for(int i=0;i<usedDBoltNum;i++){
                    usedDBolt.add(i);
                }
                jedis.set("usedDBolt".getBytes(),SerializeUtil.serialize(usedDBolt));
                //通知UBolt情况缓存
                for(int i=0;i<uBoltPhysicalIDs.size();i++){
                    Values value=new Values(SystemParameters.CTRL_MIG_END,"");
                    _collector.emitDirect(uBoltPhysicalIDs.get(i),value);
                }
                //通知DBolt更改信号量
                for(int i=0;i<usedDBoltNum;i++){
                    Values value=new Values(SystemParameters.CTRL_MIG_END,"");
                    _collector.emitDirect(dBoltPhysicalIDs.get(i),value);
                }
            }
        }else if(signal.equalsIgnoreCase(SystemParameters.TICK)){
            if(flag==false){
                reportTime++;
            }
            int dBoltIndex=dBoltPhysicalIDs.indexOf(tuple.getSourceTask());
            if(usedDBolt.contains(dBoltIndex)){
                flag=true;
                int taskLoad=tuple.getInteger(1);
                System.out.println(dBoltIndex+"号DBolt汇报负载 = "+taskLoad);
                HashMap<Integer,Integer> loadDetails=(HashMap<Integer,Integer>)(Object) SerializeUtil.unserialize(tuple.getBinary(2));
                PlanGeneratorSplitRandom.update(dBoltIndex,taskLoad,loadDetails);
                usedDBolt.remove(dBoltIndex);
                jedis.set("usedDBolt".getBytes(),SerializeUtil.serialize(usedDBolt));
                if(usedDBolt.isEmpty()){
                    flag=false;
                    PlanGeneratorSplitRandom.writeLoadToFile(bwLoad,reportTime-1);
                    boolean adjust=PlanGeneratorSplitRandom.makePlanSplitRandom();
                    if(adjust==false){
                        System.out.println("Controller收到所有负载汇报，且不需要进行负载调整");
                        PlanGeneratorSplitRandom.reset();
                        for(int i=0;i<usedDBoltNum;i++){
                            usedDBolt.add(i);
                        }
                        jedis.set("usedDBolt".getBytes(),SerializeUtil.serialize(usedDBolt));
                        for(int i=0;i<usedDBoltNum;i++){
                            Values value=new Values(SystemParameters.GOON,"");
                            _collector.emitDirect(dBoltPhysicalIDs.get(i),value);
                        }
                        for(int i=0;i<uBoltPhysicalIDs.size();i++){
                            Values value=new Values(SystemParameters.GOON,"");
                            _collector.emitDirect(uBoltPhysicalIDs.get(i),value);
                        }
                    }else{
                        System.out.println("Controller收到所有负载汇报，制定迁移计划！");
                        writeToRedis(PlanGeneratorSplitRandom.routeTable,"routetable");
                        writeToRedis(PlanGeneratorSplitRandom.endCheckInfo,"endcheckinfo");
                        PlanGeneratorSplitRandom.writeTableToFile(bwTable,reportTime-1);
                        PlanGeneratorSplitRandom.writeMigratePlan(bwTable);
                        PlanGeneratorSplitRandom.writeCheckInfo(bwTable);
                        //通知UBolt更新路由表
                        for(int i=0;i<uBoltPhysicalIDs.size();i++){
                            Values value=new Values(SystemParameters.CHANGE,"");
                            _collector.emitDirect(uBoltPhysicalIDs.get(i),value);
                        }
                        //通知DBolt，进行数据迁移
                        for(Map.Entry<Integer,ArrayList<MigrationItem>> mpi:PlanGeneratorSplitRandom.migrationPlan.entrySet()){
                            int taskIndex=mpi.getKey();
                            ArrayList<MigrationItem> list=mpi.getValue();
                            Values value=new Values(SystemParameters.CHANGE,list);
                            _collector.emitDirect(dBoltPhysicalIDs.get(taskIndex),value);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(true,new Fields(SystemParameters.SIGNAL,SystemParameters.FIELD_1));
    }

    private Jedis getConnectJedis(){
        if(jedis==null)
            jedis=new Jedis(SystemParameters.HOST_LOCAL,SystemParameters.PORT);
        return jedis;
    }

    private void writeToRedis(Object obj, String name){
        if(jedis.exists(name.getBytes())){
            jedis.del(name.getBytes());
        }
        jedis.set(name.getBytes(), SerializeUtil.serialize(obj));
    }


}

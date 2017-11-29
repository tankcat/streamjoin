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
import util.MigratePlanItem;
import util.RouteTableItem;
import tool.SerializeUtil;
import util.SystemParameters;

import java.io.BufferedWriter;
import java.util.*;

/**
 * Created by stream on 16-9-14.
 * 快速指定迁移计划，且DBolt　task实例个数固定
 */
public class QuickCtrllerBolt extends BaseRichBolt{
    private OutputCollector _collector;
    private transient Jedis jedis;
    private List<Integer> dBoltPhysicalIDs;
    private List<Integer> uBoltPhysicalIDs;
    private int UL;
    private int loadSum;
    private HashMap<Integer,HashMap<Integer,Integer>> keyLoadDetail;
    private HashMap<Integer,Integer> dBoltTaskLoad;
    private HashMap<Integer, RouteTableItem> C;
    private HashMap<Integer,ArrayList<RouteTableItem>> routeTable;
    private List<MigratePlanItem> migratePlans;
    private HashSet<Integer> usedDBolt;
    private int usedDBoltNum;
    private int curEndNum=0;
    private BufferedWriter bw;
    private boolean flag=false;
    private int k=0;
    private int BL=0;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector=outputCollector;
        jedis=getConnectJedis();
        dBoltPhysicalIDs=topologyContext.getComponentTasks(SystemParameters.DBOLT);
        uBoltPhysicalIDs=topologyContext.getComponentTasks(SystemParameters.UBOLT);
        keyLoadDetail=new HashMap<>();
        dBoltTaskLoad=new HashMap<>();
        C=new HashMap<>();
        routeTable=new HashMap<>();
        migratePlans=new ArrayList<>();
        usedDBolt=new HashSet<>();
        usedDBoltNum=SystemParameters.DBOLT_PARA_INIT;
        for(int i=0;i<usedDBoltNum;i++){
            usedDBolt.add(i);
        }
        jedis.set("usedDBolt".getBytes(), SerializeUtil.serialize(usedDBolt));
        bw= FileProcess.getWriter("Experiment/report.txt");
    }

    @Override
    public void execute(Tuple tuple) {
        String signal=tuple.getStringByField(SystemParameters.SIGNAL);
        if(signal.equalsIgnoreCase(SystemParameters.TICK)){
            if(flag==false){
                FileProcess.write("第"+(++k)+"次汇报",bw);
            }
            int dBoltIndex=dBoltPhysicalIDs.indexOf(tuple.getSourceTask());
            if(usedDBolt.contains(dBoltIndex)){
                flag=true;
                int taskLoad=tuple.getInteger(1);
                System.out.println(dBoltIndex+"号DBolt负载汇报："+taskLoad);
                FileProcess.write(dBoltIndex+"号DBolt负载汇报："+taskLoad,bw);
                HashMap<Integer,Integer> loadDetail=(HashMap<Integer,Integer>)(Object)SerializeUtil.unserialize(tuple.getBinary(2));
                jedis.set(("detail"+dBoltIndex).getBytes(),SerializeUtil.serialize(loadDetail));
                keyLoadDetail.put(dBoltIndex,loadDetail);
                dBoltTaskLoad.put(dBoltIndex,taskLoad);
                loadSum+=taskLoad;
                usedDBolt.remove(dBoltIndex);
                jedis.set("usedDBolt".getBytes(),SerializeUtil.serialize(usedDBolt));
                if(usedDBolt.isEmpty()){
                    flag=false;
                    BL=loadSum/usedDBoltNum;
                    UL=(int)(BL*(1+SystemParameters.theta));
                    System.out.println("Controller收到所有负载汇报,最大UL="+UL);
                    Iterator<Map.Entry<Integer, Integer>> taskLoadIter=dBoltTaskLoad.entrySet().iterator();
                    while(taskLoadIter.hasNext()){
                        Map.Entry<Integer,Integer> loadItem=(Map.Entry<Integer,Integer>) taskLoadIter.next();
                        int dBoltID=loadItem.getKey();
                        int dBoltLoad=loadItem.getValue();
                        //超载节点，需要进行卸载操作
                        if(dBoltLoad>UL){
                            doUnLoad(dBoltID,dBoltLoad);
                            taskLoadIter.remove();
                        }
                    }
                    if(C.size()==0){
                        System.out.println("不存在超载！");
                        initialize();
                    }else{
                        doLoad();
                        //更新路由表
                        changeRouteTable();
                        //通知UBolt更新路由表
                        System.out.println("Controller通知UBolt更新路由表！");
                        for(int i=0;i<uBoltPhysicalIDs.size();i++){
                            Values value=new Values(SystemParameters.CHANGE,"");
                            _collector.emitDirect(uBoltPhysicalIDs.get(i),value);
                        }
                        //通知DBolt，进行数据迁移
                        System.out.println("Controller通知DBolt迁移数据！");
                        for(MigratePlanItem mpi:migratePlans){
                            Values value=new Values(SystemParameters.CHANGE,mpi);
                            _collector.emitDirect(dBoltPhysicalIDs.get(mpi.getFrom()),value);
                        }
                    }
                }
            }
        }else if(signal.equalsIgnoreCase(SystemParameters.JOINER_MIG_END)){
            curEndNum++;
            if(curEndNum==migratePlans.size()){
                initialize();
                //通知UBolt清空缓存
                for(int i=0;i<uBoltPhysicalIDs.size();i++){
                    Values value=new Values(SystemParameters.CTRL_MIG_END,"");
                    _collector.emitDirect(uBoltPhysicalIDs.get(i),value);
                }
                //通知DBolt更改信号量
                for(int i=0;i<usedDBoltNum;i++){
                    Values value=new Values(SystemParameters.CTRL_MIG_END,"");
                    _collector.emitDirect(dBoltPhysicalIDs.get(i),value);
                }
                System.out.println("Controller接收到所有的迁移结束信号！");
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

    private void initialize(){
        curEndNum=0;
        loadSum=0;
        keyLoadDetail.clear();
        dBoltTaskLoad.clear();
        C.clear();
        migratePlans.clear();
        routeTable.clear();
        UL=0;
        for(int i=0;i<usedDBoltNum;i++){
            usedDBolt.add(i);
        }
        jedis.set("usedDBolt".getBytes(),SerializeUtil.serialize(usedDBolt));
    }

    private void doUnLoad(int dBoltIndex,int dBoltLoad){
        doUnloadSelect(dBoltIndex,dBoltLoad);
    }

    private void doLoad(){
        while(dBoltTaskLoad.size()>0&&C.size()>0){
            doLoadSelect();
        }
    }

    private void changeRouteTable(){
        routeTable=new HashMap<>();
        Iterator<Map.Entry<Integer, HashMap<Integer, Integer>>> keyDetailIter=keyLoadDetail.entrySet().iterator();
        while(keyDetailIter.hasNext()) {
            Map.Entry<Integer, HashMap<Integer, Integer>> detailItem = keyDetailIter.next();
            int taskIndex = detailItem.getKey();
            HashMap<Integer, Integer> keyDetails = detailItem.getValue();
            Iterator<Map.Entry<Integer, Integer>> keyDatailsIter = keyDetails.entrySet().iterator();
            while (keyDatailsIter.hasNext()) {
                Map.Entry<Integer, Integer> keyDetailItem = keyDatailsIter.next();
                int key = keyDetailItem.getKey();
                if (routeTable.containsKey(key)) {
                    routeTable.get(key).add(new RouteTableItem(keyDetailItem.getValue(), taskIndex));
                } else {
                    ArrayList<RouteTableItem> list = new ArrayList<>();
                    list.add(new RouteTableItem(keyDetailItem.getValue(), taskIndex));
                    routeTable.put(key, list);
                }
            }
        }
        writeToRedis(routeTable,"routetable");
        System.out.println("Controller制定出新的路由表！");
    }

    private void writeToRedis(Object obj, String name){
        if(jedis.exists(name.getBytes())){
            jedis.del(name.getBytes());
        }
        jedis.set(name.getBytes(),SerializeUtil.serialize(obj));
    }

    private void doUnloadSelect(int dBoltIndex,int dBoltLoad){
        HashMap<Integer,Integer> keyLoadDetails=keyLoadDetail.get(dBoltIndex);
        Iterator<Map.Entry<Integer, Integer>> keyDetailIter=keyLoadDetails.entrySet().iterator();
        while(dBoltLoad>BL&&keyDetailIter.hasNext()){
            Map.Entry<Integer, Integer> detailItem=keyDetailIter.next();
            int count=0;
            int delta=dBoltLoad-BL;
            if(delta>=detailItem.getValue()){
                count=detailItem.getValue();
                keyDetailIter.remove();
            }else{
                count=delta;
                detailItem.setValue(detailItem.getValue()-delta);
            }
            dBoltLoad-=count;
            C.put(detailItem.getKey(),new RouteTableItem(count,dBoltIndex));
        }
        recordC();
    }

    private void doLoadSelect(){
        Iterator<Map.Entry<Integer, Integer>> dBoltTaskLoadIter=dBoltTaskLoad.entrySet().iterator();
        Map.Entry<Integer,Integer> dBoltTaskLoadItem=dBoltTaskLoadIter.next();
        int toTask=dBoltTaskLoadItem.getKey();
        int load=dBoltTaskLoadItem.getValue();
        Iterator<Map.Entry<Integer,RouteTableItem>> cIter=C.entrySet().iterator();
        while(load<BL&&cIter.hasNext()){
            Map.Entry<Integer, RouteTableItem> cItem=cIter.next();
            int key=cItem.getKey();
            RouteTableItem rti=cItem.getValue();
            int delta=BL-load;
            int count=0;
            if(delta>=rti.getCount()){
                MigratePlanItem mpi=new MigratePlanItem(rti.getTaskIndex(),toTask,key,rti.getCount());
                migratePlans.add(mpi);
                count=rti.getCount();
                cIter.remove();
            }else{
                MigratePlanItem mpi=new MigratePlanItem(rti.getTaskIndex(),toTask,key,delta);
                migratePlans.add(mpi);
                count=delta;
                cItem.setValue(new RouteTableItem(rti.getCount()-delta,rti.getTaskIndex()));
            }
            load+=count;
            if(keyLoadDetail.get(toTask).containsKey(key)){
                count+=keyLoadDetail.get(toTask).get(key);
            }
            keyLoadDetail.get(toTask).put(key,count);
        }
        dBoltTaskLoadIter.remove();
        jedis.set("plan".getBytes(),SerializeUtil.serialize(migratePlans));
    }

    private void recordC(){
        BufferedWriter bw=FileProcess.getWriter("Experiment/C.txt");
        Iterator iter=C.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry<Integer,RouteTableItem> entry=(Map.Entry<Integer,RouteTableItem>)iter.next();
            int key=entry.getKey();
            RouteTableItem item=entry.getValue();
            String str="key = "+key+" , "+item.toString();
            FileProcess.write(str,bw);
        }
    }
}

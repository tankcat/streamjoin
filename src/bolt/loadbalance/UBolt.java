package bolt.loadbalance;


import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import util.RouteTableItem;
import tool.SerializeUtil;
import util.SystemParameters;

import java.util.*;

/**
 * Created by stream on 16-9-13.
 */
public class UBolt extends BaseRichBolt{
    private transient Jedis jedis=null;
    private OutputCollector _collector;
    private List<Integer> dBoltPhysicalIDs;
    private int uBoltIndex;
    private HashMap<Integer,List<String>> suspendR;
    private HashMap<Integer,List<String>> suspendS;
    private Random rand;
    private HashMap<Integer,ArrayList<RouteTableItem>> routeTable;
    private String predicate;
    private boolean isMigrating;
    private int usedDBoltNum;
    private boolean isReporting=false;
    public UBolt(String predicate){
        this.predicate=predicate;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if(jedis!=null){
            jedis.disconnect();
            jedis=null;
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector=outputCollector;
        jedis=getConnectJedis();
        routeTable=new HashMap<>();
        suspendR=new HashMap<>();
        suspendS=new HashMap<>();
        rand=new Random();
        dBoltPhysicalIDs=topologyContext.getComponentTasks(SystemParameters.DBOLT);
        uBoltIndex=topologyContext.getThisTaskIndex();
        isMigrating=false;
        usedDBoltNum =SystemParameters.DBOLT_PARA_INIT;
    }

    @Override
    public void execute(Tuple tuple) {
        if(isTickTuple(tuple)){
            if(uBoltIndex==0){
                for(int i=0;i<dBoltPhysicalIDs.size();i++){
                    Values value=new Values(SystemParameters.TICK,"","","","");
                    _collector.emitDirect(dBoltPhysicalIDs.get(i),value);
                }
            }
            isReporting=true;
        }else {
            String signal = tuple.getStringByField(SystemParameters.SIGNAL);
            //原始数据
            if (signal.equalsIgnoreCase(SystemParameters.ORI_DATA)) {
                boolean isFirst = tuple.getBooleanByField(SystemParameters.FIRST);
                int key = tuple.getIntegerByField(SystemParameters.KEY);
                String content = tuple.getStringByField(SystemParameters.CONTENT);
                if (isMigrating == false && isReporting==false) {
                    //System.out.println(content);
                    emitData(tuple, predicate, key, isFirst, content);
                } else {
                    suspendTuple(key, isFirst, content);
                }
                _collector.ack(tuple);
            }
            //路由表更改信号
            else if (signal.equalsIgnoreCase(SystemParameters.CHANGE)) {
                isMigrating = true;
                routeTable.clear();
                byte[] bytes = jedis.get("routetable".getBytes());
                Object tableObj = SerializeUtil.unserialize(bytes);
                if (tableObj != null) {
                    routeTable = (HashMap<Integer, ArrayList<RouteTableItem>>) tableObj;
                } else {
                    throw new RuntimeException("redis中不存在新的路由表信息！");
                }
                //System.out.println(uBoltIndex+"号UBolt收到路由表更新信号！");
            }
            //数据迁移结束信号，清空缓存
            else if (signal.equalsIgnoreCase(SystemParameters.CTRL_MIG_END)||signal.equalsIgnoreCase(SystemParameters.GOON)) {
                if(signal.equalsIgnoreCase(SystemParameters.CTRL_MIG_END))
                    isMigrating = false;
                if(signal.equalsIgnoreCase(SystemParameters.GOON))
                    isReporting=false;
                Iterator<Map.Entry<Integer, List<String>>> iterR = suspendR.entrySet().iterator();
                while (iterR.hasNext()) {
                    Map.Entry<Integer, List<String>> entry = iterR.next();
                    int key = entry.getKey();
                    List<String> records = entry.getValue();
                    for (String record : records) {
                        emitData(tuple, predicate, key, true, record);
                    }
                }
                Iterator<Map.Entry<Integer, List<String>>> iterS = suspendS.entrySet().iterator();
                while (iterS.hasNext()) {
                    Map.Entry<Integer, List<String>> entry = iterS.next();
                    int key = entry.getKey();
                    List<String> records = entry.getValue();
                    for (String record : records) {
                        emitData(tuple, predicate, key, false, record);
                    }
                }
                suspendR.clear();
                suspendS.clear();
                //System.out.println(uBoltIndex + "号UBolt接收到迁移结束信号，清空缓存！");
                _collector.ack(tuple);
            } else {
                throw new RuntimeException(uBoltIndex + "号UBolt接收到错误的信息！");
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(true,new Fields(SystemParameters.SIGNAL,SystemParameters.KEY,SystemParameters.FIRST,SystemParameters.CONTENT,SystemParameters.ROUTE));
    }

    private Jedis getConnectJedis(){
        if(jedis==null)
            jedis=new Jedis(SystemParameters.HOST_LOCAL, SystemParameters.PORT);
        return jedis;
    }

    private int getDestination(int key){
            List<RouteTableItem> list = routeTable.get(key);
            Collections.sort(list);
            List<Integer> temp = new ArrayList<>();
            int sum = list.get(0).getCount();
            temp.add(list.get(0).getCount());
            for (int i = 1; i < list.size(); i++) {
                temp.add(sum + list.get(i).getCount());
                sum += list.get(i).getCount();
            }
            int randNum = rand.nextInt(sum);
            for (int i = 0; i < temp.size(); i++) {
                if (randNum <= temp.get(i)) {
                    return list.get(i).getTaskIndex();
                }
            }
            throw new RuntimeException("错误的目标节点！");
    }

    private void emitData(Tuple tuple,String predicate,int key,boolean isFirst,String record){
        Values value=null;
        int dest=0;
        boolean isRouteByTable=true;
        switch (predicate){
            case "=":{
                if(routeTable.containsKey(key)) {
                    dest = getDestination(key);
                    List<RouteTableItem> indexes=routeTable.get(key);
                    for(RouteTableItem index:indexes){
                        if(index.getTaskIndex()!=dest){
                            value=new Values(SystemParameters.JOIN,key, isFirst, record,"");
                            _collector.emitDirect(dBoltPhysicalIDs.get(index.getTaskIndex()),value);
                        }
                    }
                }else{
                    dest=key%usedDBoltNum;
                    isRouteByTable=false;
                }
                value = new Values(SystemParameters.STORE, key, isFirst, record,isRouteByTable);
                _collector.emitDirect(dBoltPhysicalIDs.get(dest), tuple, value);
                break;
            }
            case "<":{
                break;
            }
        }

    }

    private void suspendTuple(int key,boolean isFirst,String content){
        if(isFirst){
            if(suspendR.containsKey(key)){
                suspendR.get(key).add(content);
            }else{
                ArrayList<String> list=new ArrayList<>();
                list.add(content);
                suspendR.put(key,list);
            }
        }else{
            if(suspendS.containsKey(key)){
                suspendS.get(key).add(content);
            }else{
                ArrayList<String> list=new ArrayList<>();
                list.add(content);
                suspendS.put(key,list);
            }
        }
    }

    @Override
    public Map<String,Object> getComponentConfiguration(){
        Map<String,Object> conf=new HashMap<String,Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, SystemParameters.EMIT_FREQUENCY);
        return conf;
    }

    private boolean isTickTuple(Tuple tuple){
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)&&
                tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}

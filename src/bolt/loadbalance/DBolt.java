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
import tool.FileProcess;
import tool.SerializeUtil;
import util.SystemParameters;
import util.loadbalance.MigrationItem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Serializable;
import java.util.*;

/**
 * Created by stream on 16-9-13.
 */
public class DBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private List<Integer> dBoltPhysicalIDs;
    private int dBoltIndex;
    private HashMap<Integer,List<String>> R_tuples;
    private HashMap<Integer,List<String>> S_tuples;
    private HashMap<Integer,Integer> load_detail;
    private HashMap<Integer,Boolean> routeByTable;
    private boolean isMigrating;
    private transient Jedis jedis;
    private int total_load;
    private int ctrlIndex;
    private boolean beginEndCheck=false;
    private int checknum=0;
    private BufferedWriter bw;
    private String type="";

    public  DBolt(String type){
        this.type=type;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector=outputCollector;
        dBoltPhysicalIDs=topologyContext.getComponentTasks(SystemParameters.DBOLT);
        dBoltIndex=topologyContext.getThisTaskIndex();
        R_tuples=new HashMap<>();
        S_tuples=new HashMap<>();
        load_detail =new HashMap<>();
        isMigrating=false;
        jedis=getConnectJedis();
        total_load=0;
        ctrlIndex=topologyContext.getComponentTasks(SystemParameters.LBCONTROLLER).get(0);
        routeByTable=new HashMap<>();
        bw= FileProcess.getWriter("Experiment/migrate"+type+".txt");
    }

    @Override
    public void execute(Tuple tuple) {
            //汇报负载
            String signal=tuple.getStringByField(SystemParameters.SIGNAL);
            if(signal.equalsIgnoreCase(SystemParameters.TICK)){
                byte[] usedDBoltByte=jedis.get("usedDBolt".getBytes());
                Object usedDBoltObj= SerializeUtil.unserialize(usedDBoltByte);
                if(usedDBoltObj!=null){
                    HashSet<Integer> usedDBolts=(HashSet<Integer>)usedDBoltObj;
                    if(usedDBolts.contains(dBoltIndex)&&isMigrating==false){
                        Values value=new Values(SystemParameters.TICK,total_load,SerializeUtil.serialize(load_detail),SerializeUtil.serialize(routeByTable));
                        _collector.emitDirect(ctrlIndex,tuple,value);
                    }
                }
            }else if(signal.equalsIgnoreCase(SystemParameters.STORE)||signal.equalsIgnoreCase(SystemParameters.JOIN)){
                int key=tuple.getIntegerByField(SystemParameters.KEY);
                boolean isFirst=tuple.getBooleanByField(SystemParameters.FIRST);
                String content=tuple.getStringByField(SystemParameters.CONTENT);
                if(signal.equalsIgnoreCase(SystemParameters.STORE)){
                    boolean isRouteByTable=tuple.getBooleanByField(SystemParameters.ROUTE);
                    storeAndJoin(key,isFirst,content,isRouteByTable);
                }else {
                    performJoin(key, isFirst, content);
                }
                _collector.ack(tuple);
            }else if(signal.equalsIgnoreCase(SystemParameters.CHANGE)){
                isMigrating=true;
                ArrayList<MigrationItem> mpiList=(ArrayList<MigrationItem>)tuple.getValue(1);
                performMigration(mpiList);
            }else if(signal.equalsIgnoreCase(SystemParameters.JOINER_MIG_EOF)){
                if(beginEndCheck==false){
                    beginEndCheck=true;
                    HashMap<Integer,Integer> endCheckInfo=(HashMap<Integer,Integer>)(Object)SerializeUtil.unserialize(jedis.get("endcheckinfo".getBytes()));
                    checknum=endCheckInfo.get(dBoltIndex);
                    checknum--;
                }else{
                    checknum--;
                }
                if(checknum==0) {
                    //System.out.println("task = "+dBoltIndex+", load = "+total_load);
                    Values value = new Values(SystemParameters.JOINER_MIG_END, "", "", "");
                    _collector.emitDirect(ctrlIndex, value);
                }
                //System.out.println(dBoltIndex+"号DBolt迁移结束");
            }else if(signal.equalsIgnoreCase(SystemParameters.MIG_DATA)){
                int key=tuple.getInteger(2);
                boolean isFirst=tuple.getBoolean(1);
                String contents=tuple.getString(3);
                getMigratedKeys(key,isFirst,contents);
            }else if(signal.equalsIgnoreCase(SystemParameters.CTRL_MIG_END)) {
                isMigrating=false;
                beginEndCheck=false;
            }else if(signal.equalsIgnoreCase(SystemParameters.GOON)){
                isMigrating=false;
            }else{
                throw new RuntimeException(dBoltIndex+"号DBolt收到错误信息！");
            }

    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(true,new Fields(SystemParameters.SIGNAL,SystemParameters.FIELD_1,SystemParameters.FIELD_2,SystemParameters.FIELD_3));
    }

    /*@Override
    public Map<String,Object> getComponentConfiguration(){
        Map<String,Object> conf=new HashMap<String,Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, SystemParameters.EMIT_FREQUENCY);
        return conf;
    }

    private boolean isTickTuple(Tuple tuple){
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)&&
                tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }*/

    private Jedis getConnectJedis(){
        if(jedis==null)
            jedis=new Jedis(SystemParameters.HOST_LOCAL, SystemParameters.PORT);
        return jedis;
    }

    private void storeAndJoin(int key, boolean isFirst, String content,boolean isRouteByTable) {
        //System.out.println(content);
        routeByTable.put(key,isRouteByTable);
        total_load++;
        if(load_detail.containsKey(key)){
            load_detail.put(key,load_detail.get(key)+1);
        }else{
            load_detail.put(key,1);
        }
        if(isFirst){
            if(R_tuples.containsKey(key)){
                R_tuples.get(key).add(content);
            }else{
                ArrayList<String> list=new ArrayList<>();
                list.add(content);
                R_tuples.put(key,list);
            }
        }else{
            if(S_tuples.containsKey(key)){
                S_tuples.get(key).add(content);
            }else{
                ArrayList<String> list=new ArrayList<>();
                list.add(content);
                S_tuples.put(key,list);
            }
        }
        performJoin(key,isFirst,content);
    }

    private void performJoin(int key,boolean isFirst,String content){
        String result=null;
        if(isFirst){
            if(S_tuples.containsKey(key)){
                List<String> list=S_tuples.get(key);
                //。。。待补充
            }
        }else{
            if(R_tuples.containsKey(key)){
                List<String> list=R_tuples.get(key);
                //。。。待补充
            }
        }
    }

   private void performMigration(ArrayList<MigrationItem> mpiList){
       //System.out.println("from = "+dBoltIndex);
        for(MigrationItem mpi:mpiList) {
            total_load -= mpi.getCount();
            int dest = mpi.getTo();
            int key = mpi.getKey();
            int mig_R;
            int mig_S;
            if (load_detail.get(key) == mpi.getCount()) {
                load_detail.remove(key);
            } else {
                load_detail.put(key, load_detail.get(key) - mpi.getCount());
            }
            String tuples = "";
            if (R_tuples.containsKey(key) && S_tuples.containsKey(key)) {
                //System.out.println("key = "+key+", R = "+R_tuples.get(key).size()+" , S = "+S_tuples.get(key).size());
                if (R_tuples.get(key).size() < mpi.getCount() / 2) {
                    List<String> tupleR = R_tuples.get(key);
                    List<String> tupleS = S_tuples.get(key);
                    mig_R = tupleR.size();
                    mig_S = mpi.getCount() - mig_R;
                    if (mig_S > tupleS.size()) {
                        System.out.println(dBoltIndex + "号DBolt存储R的数据量" + R_tuples.get(key).size() + "低于需要迁移的一半" + mpi.getCount() / 2 + "/" + mpi.getCount());
                        throw new RuntimeException(dBoltIndex + "号DBolt存储S的数据量" + S_tuples.get(key).size() + "低于需要迁移" + mig_S);
                    }
                    //迁移R
                    int k = 0;
                    for (int i = 0; i < mig_R; i++) {
                        tuples = tuples + tupleR.get(i) + "\n";
                        k++;
                        if (k == 1000) {
                            Values value = new Values(SystemParameters.MIG_DATA, true, key, tuples);
                            _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                            tuples = "";
                            k = 0;
                        }
                    }
                    if (!tuples.equals("")) {
                        Values value = new Values(SystemParameters.MIG_DATA, true, key, tuples);
                        _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                    }
                    //删除R
                    R_tuples.remove(key);
                    //迁移S
                    k = 0;
                    tuples = "";
                    for (int i = 0; i < mig_S; i++) {
                        tuples = tuples + tupleS.get(i) + "\n";
                        k++;
                        if (k == 1000) {
                            Values value = new Values(SystemParameters.MIG_DATA, false, key, tuples);
                            _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                            tuples = "";
                            k = 0;
                        }
                    }
                    if (!tuples.equals("")) {
                        Values value = new Values(SystemParameters.MIG_DATA, false, key, tuples);
                        _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                    }
                    //删除S
                    if (mig_S == tupleS.size()) {
                        S_tuples.remove(key);
                    } else {
                        tupleS = tupleS.subList(mig_S, tupleS.size());
                        S_tuples.put(key, tupleS);
                    }
                } else if (S_tuples.get(key).size() < mpi.getCount() / 2) {
                    //
                    List<String> tupleR = R_tuples.get(key);
                    List<String> tupleS = S_tuples.get(key);
                    mig_S = tupleS.size();
                    mig_R = mpi.getCount() - mig_S;
                    if (mig_R > tupleR.size()) {
                        System.out.println(dBoltIndex + "号DBolt存储S的数据量" + S_tuples.get(key).size() + "低于需要迁移的一半" + mpi.getCount() / 2 + "/" + mpi.getCount());
                        throw new RuntimeException(dBoltIndex + "号DBolt存储R的数据量" + R_tuples.get(key).size() + "低于需要迁移" + mig_R);
                    }
                    //迁移S
                    int k = 0;
                    for (int i = 0; i < mig_S; i++) {
                        tuples = tuples + tupleS.get(i) + "\n";
                        k++;
                        if (k == 1000) {
                            Values value = new Values(SystemParameters.MIG_DATA, false, key, tuples);
                            _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                            tuples = "";
                            k = 0;
                        }
                    }
                    if (!tuples.equals("")) {
                        Values value = new Values(SystemParameters.MIG_DATA, false, key, tuples);
                        _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                    }
                    //删除S
                    S_tuples.remove(key);
                    //迁移R
                    k = 0;
                    tuples = "";
                    for (int i = 0; i < mig_R; i++) {
                        tuples = tuples + tupleR.get(i) + "\n";
                        k++;
                        if (k == 1000) {
                            Values value = new Values(SystemParameters.MIG_DATA, true, key, tuples);
                            _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                            tuples = "";
                            k = 0;
                        }
                    }
                    if (!tuples.equals("")) {
                        Values value = new Values(SystemParameters.MIG_DATA, true, key, tuples);
                        _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                    }
                    //删除R
                    if (mig_R == tupleR.size()) {
                        R_tuples.remove(key);
                    } else {
                        tupleR = tupleR.subList(mig_R, tupleR.size());
                        R_tuples.put(key, tupleR);
                    }
                } else if (R_tuples.get(key).size() >= mpi.getCount()/2 && S_tuples.get(key).size() >= mpi.getCount()/2) {
                    //System.out.println(dBoltIndex+"号DBolt存储R的数据量"+R_tuples.get(key).size()+"和S的数据量"+S_tuples.get(key).size()+"均高于需要迁移的一半"+mpi.getCount()/2+"/"+mpi.getCount());
                    List<String> tupleR = R_tuples.get(key);
                    List<String> tupleS = S_tuples.get(key);
                    if(tupleR.size()>tupleS.size()) {
                        mig_R = mpi.getCount() / 2 + 1;
                        mig_S = mpi.getCount() - mig_R;
                    }else if(tupleR.size()==tupleS.size()){
                        mig_R=mpi.getCount()/2;
                        mig_S=mpi.getCount()-mig_R;
                    }else{
                        mig_S = mpi.getCount() / 2 +1;
                        mig_R=mpi.getCount()-mig_S;
                    }
                    if(mig_S>tupleS.size()||mig_R>tupleR.size()){
                        throw new RuntimeException("task = "+dBoltIndex+" , key = "+key+", count = "+mpi.getCount()+" , R = "+tupleR.size()+" , S = "+tupleS.size()+" , mig_R = "+mig_R+" , mig_S = "+mig_S);
                    }
                    //迁移R
                    int k = 0;
                    for (int i = 0; i < mig_R; i++) {
                        tuples = tuples + tupleR.get(i) + "\n";
                        k++;
                        if (k == 1000) {
                            Values value = new Values(SystemParameters.MIG_DATA, true, key, tuples);
                            _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                            tuples = "";
                            k = 0;
                        }
                    }
                    if (!tuples.equals("")) {
                        Values value = new Values(SystemParameters.MIG_DATA, true, key, tuples);
                        _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                    }
                    //迁移S
                    k = 0;
                    tuples="";
                    for (int i = 0; i < mig_S; i++) {
                        tuples = tuples + tupleS.get(i) + "\n";
                        k++;
                        if (k == 1000) {
                            Values value = new Values(SystemParameters.MIG_DATA, false, key, tuples);
                            _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                            tuples = "";
                            k = 0;
                        }
                    }
                    if (!tuples.equals("")) {
                        Values value = new Values(SystemParameters.MIG_DATA, false, key, tuples);
                        _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                    }
                    //删除R
                    if (mig_R == tupleR.size()) {
                        R_tuples.remove(key);
                    } else {
                        tupleR = tupleR.subList(mig_R, tupleR.size());
                        R_tuples.put(key, tupleR);
                    }
                    //删除S
                    if (mig_S == tupleS.size()) {
                        S_tuples.remove(key);
                    } else {
                        tupleS = tupleS.subList(mig_S, tupleS.size());
                        S_tuples.put(key, tupleS);
                    }
                }
            } else {
                //只有R_tuple存在该key
                tuples="";
                if (!S_tuples.containsKey(key)) {
                    //ystem.out.println("key = "+key+", R = "+R_tuples.get(key).size());
                    mig_R = mpi.getCount();
                    List<String> tupleR = R_tuples.get(key);
                    if (tupleR.size() < mig_R) {
                        throw new RuntimeException(dBoltIndex + "号DBolt存储R的数据量" + tupleR.size() + "低于需要迁移的" + mpi.getCount());
                    }
                    int k = 0;
                    for (int i = 0; i < mig_R; i++) {
                        tuples = tuples + tupleR.get(i) + "\n";
                        k++;
                        if (k == 1000) {
                            k = 0;
                            Values value = new Values(SystemParameters.MIG_DATA, true, key, tuples);
                            _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                            tuples = "";
                        }
                    }
                    if (!tuples.equals("")) {
                        Values value = new Values(SystemParameters.MIG_DATA, true, key, tuples);
                        _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                    }
                    if (tupleR.size() == mpi.getCount()) {
                        R_tuples.remove(key);
                    } else {
                        tupleR = tupleR.subList(mig_R, tupleR.size());
                        R_tuples.put(key, tupleR);
                    }
                } else {
                    //只有S_tuple存在该key
                    //System.out.println("key = "+key+", S = "+S_tuples.get(key).size());
                    mig_S = mpi.getCount();
                    List<String> tupleS = S_tuples.get(key);
                    if (tupleS.size() < mig_S) {
                        throw new RuntimeException(dBoltIndex + "号DBolt存储S的数据量" + tupleS.size() + "低于需要迁移的" + mpi.getCount());
                    }
                    int k = 0;
                    for (int i = 0; i < mig_S; i++) {
                        tuples = tuples + tupleS.get(i) + "\n";
                       k++;
                        if (k == 1000) {
                            k = 0;
                            Values value = new Values(SystemParameters.MIG_DATA, false, key, tuples);
                            _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                            tuples = "";
                        }
                    }
                    if (!tuples.equals("")) {
                        Values value = new Values(SystemParameters.MIG_DATA, false, key, tuples);
                        _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
                    }
                    if (tupleS.size() == mpi.getCount()) {
                        S_tuples.remove(key);
                    } else {
                        tupleS = tupleS.subList(mig_S, tupleS.size());
                        S_tuples.put(key, tupleS);
                    }
                }
            }
            Values value = new Values(SystemParameters.JOINER_MIG_EOF, "", "", "");
            _collector.emitDirect(dBoltPhysicalIDs.get(dest), value);
        }
    }

    private void getMigratedKeys(int key,boolean isFirst,String content){
        String[] contents=content.split("\n");
        if(contents==null||contents.length==0)
            return;
        total_load+=contents.length;
        //System.out.println("task = "+dBoltIndex+", key = "+key+" , count = "+contents.length);
        FileProcess.write("task = "+dBoltIndex+", key = "+key+" , count = "+contents.length,bw);
        if(load_detail.containsKey(key)){
            load_detail.put(key,contents.length+load_detail.get(key));
        }else{
            load_detail.put(key,contents.length);
        }
        //迁移来R
        if(isFirst){
            if(R_tuples.containsKey(key)){
                for(int i=0;i<contents.length;i++){
                    R_tuples.get(key).add(contents[i]);
                }
            }else{
                ArrayList<String> list=new ArrayList<>();
                for(int i=0;i<contents.length;i++){
                    list.add(contents[i]);
                }
                R_tuples.put(key,list);
            }
        }
        //迁移来S
        else{
            if(S_tuples.containsKey(key)){
                for(int i=0;i<contents.length;i++){
                    S_tuples.get(key).add(contents[i]);
                }
            }else{
                ArrayList<String> list=new ArrayList<>();
                for(int i=0;i<contents.length;i++){
                    list.add(contents[i]);
                }
                S_tuples.put(key,list);
            }
        }
    }

    public static void main(String[] args){
        List<Integer> list=new ArrayList<>();
        for(int i=10;i>0;i--){
            list.add(i);
        }
        list=list.subList(1,10);
        for(Integer i:list){
            System.out.print(i+" ");
        }

    }
}

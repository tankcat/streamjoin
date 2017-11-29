package bolt.thetajoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import redis.clients.jedis.Jedis;
import tool.SystemParameters;
import tool.TableItemForJoin;
import util.MigratePlanItem;
import tool.SerializeUtil;

public class ControllerLBBoltForJoin extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4986612676880797960L;
	
	private String host;
	private int port;
	private transient Jedis jedis=null;
	
	private OutputCollector collector;
	
	private List<Integer> dBoltPhysicalIds;
	private List<Integer> uBoltPhysicalIds;
    
    private boolean beingMigrating=false;
    private int boltPara;
	private int calculative;
    private double loadSum=0;
    private double BL=0;
   /* @SuppressWarnings("unchecked")
	private  HashMap<Integer,Integer>[] keyLoadInfos=new HashMap[SystemParameters.dBoltPara];*/
    private HashMap<Integer,Map<Integer,Integer>> keyLoadInfos;
   /* @SuppressWarnings("unchecked")
	private  HashMap<Integer,Integer>[] backup=new HashMap[SystemParameters.dBoltPara];*/
    private HashMap<Integer,Integer> taskLoads;
    private HashMap<Integer,TableItemForJoin> C;
    private List<MigratePlanItem> migratePlans;
    
    private HashMap<Integer,List<TableItemForJoin>> routingTable;
    private String name;
    private int report=0;
    private int migrateIndex=0;
    private boolean beingReport=false;
    
    public ControllerLBBoltForJoin(String host, int port,String name) {
		 this.host = host;
		 this.port = port;
		 this.name=name;
	}

	@SuppressWarnings({ "rawtypes"})
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		 this.collector = collector;
		 jedis= getConnectJedis();
		 dBoltPhysicalIds=context.getComponentTasks(SystemParameters.dBoltId);
		 uBoltPhysicalIds=context.getComponentTasks(SystemParameters.uBoltId);
		 taskLoads=new HashMap<Integer,Integer>();
		 C=new HashMap<Integer,TableItemForJoin>();
		 migratePlans=new ArrayList<MigratePlanItem>();
		 routingTable=new HashMap<Integer,List<TableItemForJoin>>();
		 keyLoadInfos=new HashMap<Integer,Map<Integer,Integer>>();
		 boltPara=SystemParameters.dBoltPara;
		 report=0;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		if(isTickTuple(input)&&boltPara==SystemParameters.dBoltPara && beingMigrating==false&&beingReport==false){
			beingReport=true;
			System.out.println("controller触发汇报.");
			for(int i=0;i<SystemParameters.dBoltPara;i++){
				Values values=new Values(SystemParameters.report,"");
				collector.emitDirect(dBoltPhysicalIds.get(i), input,values);
			}
		}else{
		String signal="";
		try{
			signal=input.getStringByField(SystemParameters.signal);
		}catch (IllegalArgumentException e) {
			System.out.println(input.getSourceComponent()+":"+dBoltPhysicalIds.indexOf(input.getSourceTask()));
		}
		if(signal.equals(SystemParameters.keyLoadInfo)){
			int taskIndex=input.getIntegerByField(SystemParameters.fieldOption1);
			String Name=name+taskIndex;
			HashMap<Integer, Integer> keyLoadInfo=null;
			if(jedis.exists(Name.getBytes())){
				byte[] bytes=jedis.get(Name.getBytes());
				Object object=SerializeUtil.unserialize(bytes);
				if(object!=null)
					keyLoadInfo=(HashMap<Integer, Integer>)object;
			}else{
				System.out.println(taskIndex+"号DBolt未把信息写入redis.");
				initialize();
				collector.ack(input);
				return;
			}
			keyLoadInfos.put(taskIndex,keyLoadInfo);
			int taskLoad=input.getIntegerByField(SystemParameters.fieldOption2);
			loadSum+=taskLoad;
			taskLoads.put(taskIndex, taskLoad);
			boltPara--;
			if( boltPara==0){
				beingReport=false;
				report++;
				jedis.set("report", report+"");
				BL= loadSum/SystemParameters.dBoltPara*(1+SystemParameters.thetaBalance);
				loadSum=0;
				System.out.println("BL = "+ BL);
				System.out.println("controller接收到所有的负载汇报信息,开始制定迁移计划.");
				boltPara=SystemParameters.dBoltPara;
				Iterator<Entry<Integer, Integer>> iter= taskLoads.entrySet().iterator();
				int migrateCount=0;
				long begin=System.currentTimeMillis();
				while(iter.hasNext()){
					Map.Entry<Integer, Integer> entry=(Entry<Integer, Integer>) iter.next();
					int dBoltTask=entry.getKey();
					int load=entry.getValue();
					if(load> BL){
						migrateCount=migrateCount+(int)(load-BL);
						unload(dBoltTask);
						iter.remove();
					}
				}
				if( C.size()==0){
					System.out.println("不需要迁移.");
					initialize();
					collector.ack(input);
					return;
				}
				migrateIndex++;
				jedis.lpush("migrateCount", migrateIndex+"-"+migrateCount);
				load();
				System.out.println("plan条目："+ migratePlans.size());
				 writeToRedis( migratePlans, "plan");
				 getRoutingTable(migrateIndex);
				 long migrateTime=System.currentTimeMillis()-begin;
				 jedis.lpush("migrateTime", migrateIndex+"-"+migrateTime);
				 beingMigrating=true;
				//发送迁移通知
					//通知UBolt更新routingTable
					if(jedis.exists(SystemParameters.routingTable.getBytes())){
						jedis.del(SystemParameters.routingTable.getBytes());
					}
					 writeToRedis( routingTable, SystemParameters.routingTable);
					for(int i=0;i<SystemParameters.uBoltPara;i++){
						 collector.emitDirect( uBoltPhysicalIds.get(i),input,new Values(SystemParameters.updateTable,""));
					}
					System.out.println("controller向UBolt通知更新routingTable.");
					//通知DBolt进行key的迁移
					for(MigratePlanItem mpi: migratePlans){
						Values value=new Values(SystemParameters.migrateSignal,mpi);
						collector.emitDirect( dBoltPhysicalIds.get(mpi.getFrom()), input,value);
					}
					System.out.println("controller向DBolt通知迁移计划.");
				}
		}else if(signal.equals(SystemParameters.completeMigrate)){
			 calculative++;
			if( calculative== migratePlans.size()){
				 C=new HashMap<Integer,TableItemForJoin>();
				 keyLoadInfos=new HashMap<Integer,Map<Integer,Integer>>();
				 taskLoads=new HashMap<Integer,Integer>();
				 migratePlans.clear();
				 calculative=0;
				 beingMigrating=false;
				for(int i=0;i<SystemParameters.uBoltPara;i++){
					Values value=new Values(SystemParameters.completeMigrate,"");
					 collector.emitDirect( uBoltPhysicalIds.get(i),input,value);
				}
				System.out.println("controller接收到所有迁移结束信号，通知UBolt清空缓存.");
			}
		}
		collector.ack(input);
		}
	}

	private void initialize(){
		 boltPara=SystemParameters.dBoltPara;
		 keyLoadInfos=new HashMap<Integer,Map<Integer,Integer>>();
		 taskLoads=new HashMap<Integer,Integer>();
		 beingMigrating=false;
		 beingReport=false;
		 loadSum=0;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(SystemParameters.signal,SystemParameters.fieldOption1));
	}
	
	@Override
	public void cleanup(){
		 getDisconnectJedis();
	}
	
	private Jedis getConnectJedis(){
		if(jedis==null)
			jedis=new Jedis(host,port);
		return jedis;
	}
	
	private void getDisconnectJedis(){
		if(jedis!=null){
			jedis.disconnect();
			jedis=null;
		}
	}
	
	private void unload(int taskIndex){
		int load= taskLoads.get(taskIndex);
		unloadSelect(taskIndex,load);
	}
	
	private void load(){
		while(taskLoads.size()>0 &&  C.size()>0){
			loadSelect();
		}
	}
	
	private void unloadSelect(int taskIndex,int load){
		Iterator<Entry<Integer, Integer>> iter=keyLoadInfos.get(taskIndex).entrySet().iterator();
		while(load> BL && iter.hasNext()){
			Map.Entry<Integer, Integer> entry=(Entry<Integer, Integer>) iter.next();
			int delta=load-(int) BL;
			int migrateCount=0;
			if(delta>=entry.getValue()){
				migrateCount=entry.getValue();
				iter.remove();
			}else{
				migrateCount=delta;
				entry.setValue(entry.getValue()-delta);
			}
			load-=migrateCount;
			C.put(entry.getKey(), new TableItemForJoin(migrateCount,taskIndex));
		}
	}
	
	private void loadSelect(){
		Iterator<Entry<Integer, Integer>> iter= taskLoads.entrySet().iterator();
		Map.Entry<Integer, Integer> entry=iter.next();
		int toTask=entry.getKey();
		int load=entry.getValue();
		Iterator<Entry<Integer, TableItemForJoin>> p= C.entrySet().iterator();
		while(load<= BL && p.hasNext()){
			Map.Entry<Integer, TableItemForJoin> e=p.next();
			int key=e.getKey();
			TableItemForJoin ti=e.getValue();
			int delta=(int) BL-load;
			int addCount=0;
			if(delta>=ti.count){
				MigratePlanItem mpi=new MigratePlanItem(ti.taskIndex,toTask,key,ti.count);
				 migratePlans.add(mpi);
				addCount=ti.count;
				p.remove();
			}else{
				MigratePlanItem mpi=new MigratePlanItem(ti.taskIndex,toTask,key,delta);
				 migratePlans.add(mpi);
				addCount=delta;
				e.setValue(new TableItemForJoin(ti.count-delta,ti.taskIndex));
			}
			if(keyLoadInfos.get(toTask).containsKey(key)){
				 addCount=addCount+keyLoadInfos.get(toTask).get(key);
			}
			keyLoadInfos.get(toTask).put(key, addCount);
		}
		iter.remove();
	}
	
	private void getRoutingTable(int index){
		routingTable=new HashMap<Integer,List<TableItemForJoin>>();
		int size=0;
		Iterator<Entry<Integer, Map<Integer, Integer>>> iter=keyLoadInfos.entrySet().iterator();
		while(iter.hasNext()){
			Map.Entry<Integer, Map<Integer,Integer>> e=iter.next();
			int taskIndex=e.getKey();
			Map<Integer,Integer> keyInfo=e.getValue();
			Iterator<Entry<Integer, Integer>> p=keyInfo.entrySet().iterator();
			while(p.hasNext()){
				Map.Entry<Integer, Integer> ee=p.next();
				int key=ee.getKey();
				if(this.routingTable.containsKey(key)){
					size+=2;
					this.routingTable.get(key).add(new TableItemForJoin(ee.getValue(),taskIndex));
				}else{
					size+=3;
					List<TableItemForJoin> list=new ArrayList<TableItemForJoin>();
					list.add(new TableItemForJoin(ee.getValue(),taskIndex));
					this.routingTable.put(key, list);
				}
			}
		}
		jedis.lpush("routeTableSize", index+"-"+size*4+"Bytes");
	}
	
	private void writeToRedis(Object obj,String name){
		if(jedis.exists(name.getBytes()))
			jedis.del(name.getBytes());
		jedis.set(name.getBytes(), SerializeUtil.serialize(obj));
	}
	
	
	public static void main(String[] args){
		HashMap<Integer,Integer> test=new HashMap<Integer,Integer>();
		for(int i=0;i<3;i++){
			test.put(i, i);
		}
		Test(test);
		System.out.println(test.containsKey(1));
		System.out.println(test.size());
	}
	
	public static void Test(HashMap<Integer,Integer> test){
		test.remove(1);
	}
	
	private boolean isTickTuple(Tuple tuple){
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)&&
				tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
}

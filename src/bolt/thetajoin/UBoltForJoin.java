package bolt.thetajoin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import redis.clients.jedis.Jedis;
import tool.FuncCollections;
import tool.SystemParameters;
import tool.TableItemForJoin;
import tool.SerializeUtil;

public class UBoltForJoin extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -848668669189578661L;
	private String host;
	private int port;
	private transient Jedis jedis=null;
	private boolean beingMigrate=false;
	
	private List<Integer> dBoltPhysicalIds;
	private int taskIndex;
	
	private OutputCollector collector;
	
	private HashMap<Integer,List<TableItemForJoin>> routingTable;
	private HashMap<Integer, List<String>> suspendForR=null;
	private HashMap<Integer, List<String>> suspendForS=null;
	private Random rand;
	private int range;
	private int bcCount=0;

	public UBoltForJoin(String host,int port,int range){
		this.host=host;
		this. port=port;
		this.range=range;
	}
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		jedis= getConnectJedis();
		this.collector=collector;
		 routingTable=new HashMap<Integer,List<TableItemForJoin>>();
		 suspendForR=new HashMap<Integer,List<String>>();
		 suspendForS=new HashMap<Integer,List<String>>();
		 dBoltPhysicalIds=context.getComponentTasks(SystemParameters.dBoltId);
		 rand=new Random();
		 taskIndex=context.getThisTaskIndex();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		if(TupleUtils.isTick(input))
			return;
		String signal=input.getStringByField(SystemParameters.signal);
		//1: 正常的tuple
		if(signal.equals(SystemParameters.tuple)){
			String rel=input.getStringByField(SystemParameters.relation);
			String text=input.getStringByField(SystemParameters.text);
			String[] items=text.split("\\|");
			int orderkey=Integer.parseInt(items[0]);
			int quantity=Integer.parseInt(items[4]);
			String shipmode=items[14];
			String shipinstruct=items[13];
			//处于迁移状态，对tuple进行缓存
			if( beingMigrate==true){
				if(rel.equalsIgnoreCase("R")){
					if( suspendForR.containsKey(orderkey)){
						 suspendForR.get(orderkey).add(text);
					}else{
						ArrayList<String> list=new ArrayList<String>();
						list.add(text);
						 suspendForR.put(orderkey, list);
					}
				}else{
					if( suspendForS.containsKey(orderkey)){
						 suspendForS.get(orderkey).add(text);
					}else{
						ArrayList<String> list=new ArrayList<String>();
						list.add(text);
						 suspendForS.put(orderkey, list);
					}
				}
			}
			//正常发送tuple:join & store
			else{
				//store
				Values value=new Values(SystemParameters.store,rel,orderkey,text);
				int dest=0;
				if( routingTable.containsKey(orderkey)){
					dest= getDestination(orderkey);
				}else{
					dest=orderkey%SystemParameters.dBoltPara;
				}
				bcCount++;
				collector.emitDirect( dBoltPhysicalIds.get(dest),input, value);
				 
				//join
				if(rel.equalsIgnoreCase("R")){
					if(shipmode.equalsIgnoreCase("TRUK")){
						 emitForJoin(orderkey, rel, text,input);
					}
				}else{
					if(shipinstruct.equalsIgnoreCase("NONE") && quantity>48){
						 emitForJoin(orderkey, rel, text,input);
					}
				}
			}
			 collector.ack(input);
		}
		//2: 迁移通知，更新路由表
		else if(signal.equals(SystemParameters.updateTable)){
			 beingMigrate=true;
			 routingTable.clear();
			byte[] bytes=jedis.get(SystemParameters.routingTable.getBytes());
			Object obj=SerializeUtil.unserialize(bytes);
			if(obj!=null)
				 routingTable=(HashMap<Integer,List<TableItemForJoin>>)obj;
			System.out.println( taskIndex+"号UBolt接收到迁移信号，更新路由表.");
		}
		//3: 迁移结束通知，清空缓存
		else if(signal.equals(SystemParameters.completeMigrate)){
			//System.out.println( context.getThisTaskIndex()+"号Ubolt接收到迁移结束信号.");
			long begin=System.currentTimeMillis();
			 beingMigrate=false;
			int dest=0;
			Iterator<Entry<Integer, List<String>>> iterR= suspendForR.entrySet().iterator();
			while(iterR.hasNext()){
				Map.Entry<Integer, List<String>> entry=iterR.next();
				int orderkey=entry.getKey();
				List<String> tuples=entry.getValue();
				if( routingTable.containsKey(orderkey)){
					dest= getDestination(orderkey);
				}else{
					dest=orderkey%SystemParameters.dBoltPara;
				}
				for(String tuple:tuples){
					bcCount++;
					String[] items=tuple.split("\\|");
					String shipmode=items[14];
					Values value=new Values(SystemParameters.store,"R",orderkey,tuple);
					 collector.emitDirect(dBoltPhysicalIds.get(dest), value);
					if(shipmode.equalsIgnoreCase("TRUK")){
						 emitForJoin(orderkey, "R",tuple,input);
					}
				}
			}
			Iterator<Entry<Integer, List<String>>> iterS= suspendForS.entrySet().iterator();
			while(iterS.hasNext()){
				Map.Entry<Integer, List<String>> entry=iterS.next();
				int orderkey=entry.getKey();
				List<String> tuples=entry.getValue();
				if( routingTable.containsKey(orderkey)){
					dest= getDestination(orderkey);
				}else{
					dest=orderkey%SystemParameters.dBoltPara;
				}
				for(String tuple:tuples){
					bcCount++;
					String[] items=tuple.split("\\|");
					int quantity=Integer.parseInt(items[4]);
					String shipinstruct=items[13];
					Values value=new Values(SystemParameters.store,"S",orderkey,tuple);
					 collector.emitDirect( dBoltPhysicalIds.get(dest), value);
					if(shipinstruct.equalsIgnoreCase("NONE") && quantity>48){
						 emitForJoin(orderkey, "S", tuple,input);
					}
				}
			}
			long end=System.currentTimeMillis()-begin;
			System.out.println(taskIndex+"号bolt清空缓存耗时："+end);
			 collector.ack(input);
		}
		else if(signal.equals(SystemParameters.sendEnd)){
			jedis.lpush("broadcast", taskIndex+"-"+bcCount);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(SystemParameters.signal,SystemParameters.relation,SystemParameters.key,SystemParameters.text));
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

	private int getDestination(int key){
		List<TableItemForJoin> list= routingTable.get(key);
		Collections.sort(list);
		List<Integer> temp=new ArrayList<Integer>();
		int sum=list.get(0).count;
		temp.add(list.get(0).count);
		for(int i=1;i<list.size();i++){
			temp.add(sum+list.get(i).count);
			sum+=list.get(i).count;
		}
		int randNum= rand.nextInt(sum);
		for(int i=0;i<temp.size();i++){
			if(randNum<temp.get(i)){
				return list.get(i).taskIndex;
			}
		}
		return -1;
	}
	

	private void emitForJoin(int orderkey,String relation,String text,Tuple input){
		int dest=0;
		Values value;
		List<Integer> oppositeKeys=FuncCollections.getKeys(orderkey, range);
		for(Integer key:oppositeKeys){
			if( routingTable.containsKey(key)){
				dest= getDestination(key);
			}else{
				dest=key%SystemParameters.dBoltPara;
			}
			bcCount++;
			value=new Values(SystemParameters.join,relation,orderkey,text);
			 collector.emitDirect( dBoltPhysicalIds.get(dest), input,value);
			//System.out.println("forjoin");
		}
	}
}

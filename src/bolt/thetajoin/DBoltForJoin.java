package bolt.thetajoin;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import util.MigratePlanItem;
import tool.SerializeUtil;

public class DBoltForJoin extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6325911739457402632L;
	
	private String host;
	private int port;
	private transient Jedis jedis=null;
	private int range;
	private String name;
	
	private OutputCollector collector;
	private TopologyContext context;
	private int taskIndex=0;
	private List<Integer> dBoltPhysicalIds;
	
	private int totalLoad=0;
	
	private Map<Integer,Integer> total_keyloads;
	private Map<Integer,Integer> R_keyloads;
	private Map<Integer,Integer> S_keyloads;
	private Map<Integer,List<String>> R_tuples;
	private Map<Integer,List<String>> S_tuples;
	
	private int time=0;
	
	private String tupleTime;
	private int report=0;
	
	public DBoltForJoin(String host,int port,int range,String name){
		 this.host=host;
		 this.port=port;
		 this.range=range;
		 this.name=name;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		 this.context=context;
		 this.collector=collector;
		 jedis= getConnectJedis();
		 taskIndex=context.getThisTaskIndex();
		 dBoltPhysicalIds=context.getComponentTasks(SystemParameters.dBoltId);
		 R_keyloads=new HashMap<Integer,Integer>();
		 S_keyloads=new HashMap<Integer,Integer>();
		 R_tuples=new HashMap<Integer,List<String>>();
		 S_tuples=new HashMap<Integer,List<String>>();
		 total_keyloads=new HashMap<Integer,Integer>();
		 //bw=FileProcess.getWriter("report.txt");
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		if(TupleUtils.isTick(input))
			return;
		String signal=input.getStringByField(SystemParameters.signal);
		if(signal.equals(SystemParameters.report)){
			int ctrlReport=Integer.parseInt(jedis.get("report"));
			if(report!=ctrlReport)
				return;
			//FileProcess.write("第"+time+"次 "+taskIndex+"号DBolt tick "+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+", 前一个tuple: "+tupleTime, bw);
			System.out.println(taskIndex+"号DBolt汇报负载:"+totalLoad);
			time++;
			String Name=name+taskIndex;
			if(jedis.exists(Name.getBytes()))
				jedis.del(Name.getBytes());
			jedis.set(Name.getBytes(), SerializeUtil.serialize(total_keyloads));
			Values value=new Values(SystemParameters.keyLoadInfo,taskIndex,totalLoad,"");
			collector.emitDirect(context.getComponentTasks(SystemParameters.ctrlBoltId).get(0), input,value);
			collector.ack(input);
			report++;
		}
		else if(signal.equalsIgnoreCase(SystemParameters.store)){
			String rel=input.getStringByField(SystemParameters.relation);
			int key=input.getIntegerByField(SystemParameters.key);
			String text=input.getStringByField(SystemParameters.text);
			storeTuple(rel, key, text);
			tupleTime=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+" forstore";
		}
		else if(signal.equalsIgnoreCase(SystemParameters.join)){
			String rel=input.getStringByField(SystemParameters.relation);
			int key=input.getIntegerByField(SystemParameters.key);
			String text=input.getStringByField(SystemParameters.text);
			List<Integer> keys=FuncCollections.getKeys(key, range);
			if(rel.equalsIgnoreCase("R")){
				for(Integer oppositeKey:keys){
					if( S_tuples.containsKey(oppositeKey)){
						List<String> tuplesS=S_tuples.get(oppositeKey);
						for(String tupleS:tuplesS){
							String[] items=tupleS.split("\\|");
							String shipinstruct=items[13];
							int quantity=Integer.parseInt(items[4]);
							if(quantity>48 && shipinstruct.equalsIgnoreCase("NONE"))
								jedis.lpush("joinresult", text+"|"+tupleS);
						}
					}
				}
			}else{
				for(Integer oppositeKey:keys){
					if( R_tuples.containsKey(oppositeKey)){
						List<String> tuplesR=R_tuples.get(oppositeKey);
						for(String tupleR:tuplesR){
							String[] items=tupleR.split("\\|");
							String shipmode=items[14];
							if(shipmode.equalsIgnoreCase("TRUK"))
								jedis.lpush("joinresult", tupleR+"|"+text);
						}
					}
				}
			}
			tupleTime=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())+" forjoin";
		}
		else if(signal.equalsIgnoreCase(SystemParameters.migrateSignal)){
			MigratePlanItem mpi=(MigratePlanItem) input.getValue(1);
			 doMigration(mpi,input);
			 totalLoad-=mpi.getCount();
		}
		else if(signal.equalsIgnoreCase(SystemParameters.migratedKeys)){
			int key=input.getIntegerByField(SystemParameters.fieldOption2);
			String rel=input.getStringByField(SystemParameters.fieldOption1);
			String content=input.getStringByField(SystemParameters.fieldOption3);
			String[] texts=content.split("\n");
			if(total_keyloads.containsKey(key))
				total_keyloads.put(key, total_keyloads.get(key)+texts.length);
			else
				total_keyloads.put(key, texts.length);
			if(rel.equals("R")){
				if( R_keyloads.containsKey(key)){
					R_keyloads.put(key, R_keyloads.get(key)+texts.length);
					for(String text:texts)
						R_tuples.get(key).add(text);
				}else{
					 R_keyloads.put(key, texts.length);
					List<String> list=new ArrayList<String>();
					for(String text:texts){
						list.add(text);
						 R_tuples.put(key, list);
					}
				}
			}else{
				if( S_keyloads.containsKey(key)){
					 S_keyloads.put(key, S_keyloads.get(key)+texts.length);
					 for(String text:texts)
						 S_tuples.get(key).add(text);
				}else{
					S_keyloads.put(key, texts.length);
					List<String> list=new ArrayList<String>();
					for(String text:texts){
						list.add(text);
						S_tuples.put(key, list);
					}
				}
			}
		}else if(signal.equalsIgnoreCase(SystemParameters.completeMigrate)){
			 collector.emitDirect(context.getComponentTasks(SystemParameters.ctrlBoltId).get(0), input,new Values(SystemParameters.completeMigrate,"","",""));
			//System.out.println( taskIndex+"号DBolt接收到迁移结束信号，通知Controller.");
		}
		 collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(SystemParameters.signal,SystemParameters.fieldOption1,SystemParameters.fieldOption2,SystemParameters.fieldOption3));
	}
	
	@Override
	public void cleanup(){
		 getDisconnectJedis();
	}
	
	/*@Override
	public Map<String,Object> getComponentConfiguration(){
		Map<String,Object> conf=new HashMap<String,Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, SystemParameters.emitFrequencyInSeconds);
		return conf;
	}*/
	
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
	
	private void storeTuple(String rel,int key,String text){
		 totalLoad++;
		if( total_keyloads.containsKey(key)){
			 total_keyloads.put(key,  total_keyloads.get(key)+1);
		}else{
			 total_keyloads.put(key, 1);
		}
		if(rel.equals("R")){
			if( R_keyloads.containsKey(key)){
				 R_keyloads.put(key, R_keyloads.get(key)+1);
				 R_tuples.get(key).add(text);
			}else{
				 R_keyloads.put(key, 1);
				List<String> tuples=new ArrayList<String>();
				tuples.add(text);
				 R_tuples.put(key, tuples);
			} 
		}else{
			if( S_keyloads.containsKey(key)){
				 S_keyloads.put(key, S_keyloads.get(key)+1);
				 S_tuples.get(key).add(text);
			}else{
				 S_keyloads.put(key, 1);
				List<String> tuples=new ArrayList<String>();
				tuples.add(text);
				 S_tuples.put(key, tuples);
			} 
		}
	}
	
	private void doMigration(MigratePlanItem mpi,Tuple input){
		int dest=mpi.getTo();
		int migrateKey=mpi.getKey();
		int migrateR=0;
		int migrateS=0;
		if( total_keyloads.get(migrateKey)==mpi.getCount()){
			 total_keyloads.remove(migrateKey);
		}else{
			 total_keyloads.put(migrateKey,  total_keyloads.get(migrateKey)-mpi.getCount());
		}
		String tuples="";
		if( R_tuples.containsKey(migrateKey)&& S_tuples.containsKey(migrateKey)){
			List<String> tuplesR=R_tuples.get(migrateKey);
			List<String> tuplesS=S_tuples.get(migrateKey);
			int R_length=tuplesR.size();
			int S_length=tuplesS.size();
			if(R_length<S_length){
				if(tuplesR.size()>mpi.getCount()/2){
					migrateR=mpi.getCount()/2;
					for(int i=tuplesR.size()-1;i>=tuplesR.size()-migrateR;i--){
						tuples=tuples+tuplesR.get(i)+'\n';
					}
					Values value=new Values(SystemParameters.migratedKeys,"R",migrateKey,tuples);
					collector.emitDirect( dBoltPhysicalIds.get(dest), input,value);
					tuplesR=tuplesR.subList(0, tuplesR.size()-migrateR);
					 R_tuples.put(migrateKey, tuplesR);
					 R_keyloads.put(migrateKey, tuplesR.size());
				}else{
					migrateR=tuplesR.size();
					for(int i=tuplesR.size()-1;i>=0;i--){
						tuples=tuples+tuplesR.get(i)+'\n';
						
					}
					Values value=new Values(SystemParameters.migratedKeys,"R",migrateKey,tuples);
					collector.emitDirect( dBoltPhysicalIds.get(dest), input,value);
					 R_tuples.remove(migrateR);
					 R_keyloads.remove(migrateKey);
				}
				migrateS=mpi.getCount()-migrateR;
				tuples="";
				for(int i=tuplesS.size()-1;i>=tuplesS.size()-migrateS;i--){
					tuples=tuples+tuplesS.get(i)+'\n';
				}
				Values value=new Values(SystemParameters.migratedKeys,"S",migrateKey,tuples);
				collector.emitDirect( dBoltPhysicalIds.get(dest), input,value);
					
				if(migrateS==tuplesS.size()){
					 S_keyloads.remove(migrateKey);
					 S_tuples.remove(migrateKey);
				}else{
					tuplesS=tuplesS.subList(0, tuplesS.size()-migrateS);
					 S_tuples.put(migrateKey, tuplesS);
					 S_keyloads.put(migrateKey,tuplesS.size());
				}
			}else{
				if(tuplesS.size()>mpi.getCount()/2){
					migrateS=mpi.getCount()/2;
					for(int i=tuplesS.size()-1;i>=tuplesS.size()-migrateS;i--){
						tuples=tuples+tuplesS.get(i)+'\n';
					}
					Values value=new Values(SystemParameters.migratedKeys,"S",migrateKey,tuples);
					 collector.emitDirect( dBoltPhysicalIds.get(dest), input,value);
					tuplesS=tuplesS.subList(0, tuplesS.size()-migrateS);
					 S_tuples.put(migrateKey, tuplesS);
					 S_keyloads.put(migrateKey, tuplesS.size());
				}else{
					migrateS=tuplesS.size();
					for(int i=tuplesS.size()-1;i>=0;i--){
						tuples=tuples+tuplesS.get(i)+'\n';
					}
					Values value=new Values(SystemParameters.migratedKeys,"S",migrateKey,tuples);
					 collector.emitDirect( dBoltPhysicalIds.get(dest),input, value);
					 S_tuples.remove(migrateS);
					 S_keyloads.remove(migrateKey);
				}
				migrateR=mpi.getCount()-migrateS;
				tuples="";
				for(int i=tuplesR.size()-1;i>=tuplesR.size()-migrateR;i--){
					tuples=tuples+tuplesR.get(i)+'\n';
				}
				Values value=new Values(SystemParameters.migratedKeys,"R",migrateKey,tuples);
				 collector.emitDirect( dBoltPhysicalIds.get(dest),input, value);
			
				if(migrateR==tuplesR.size()){
					 R_keyloads.remove(migrateKey);
					 R_tuples.remove(migrateKey);
				}else{
					tuplesR=tuplesR.subList(0, tuplesR.size()-migrateR);
					 R_tuples.put(migrateKey, tuplesR);
					 R_keyloads.put(migrateKey,tuplesR.size());
				}
			}	
		}else{
			if(!R_tuples.containsKey(migrateKey)){
				migrateS=mpi.getCount();
				List<String> tuplesS=S_tuples.get(migrateKey);
				for(int i=tuplesS.size()-1;i>=tuplesS.size()-migrateS;i--){
					tuples=tuples+tuplesS.get(i)+'\n';
					}
				Values value=new Values(SystemParameters.migratedKeys,"S",migrateKey,tuples);
				 collector.emitDirect( dBoltPhysicalIds.get(dest), input,value);
			
				if(migrateS==tuplesS.size()){
					 S_keyloads.remove(migrateKey);
					 S_tuples.remove(migrateKey);
				}else{
					tuplesS=tuplesS.subList(0, tuplesS.size()-migrateS);
					 S_tuples.put(migrateKey, tuplesS);
					 S_keyloads.put(migrateKey,tuplesS.size());
				}
			}else{
				migrateR=mpi.getCount();
				List<String> tuplesR=R_tuples.get(migrateKey);
				for(int i=tuplesR.size()-1;i>=tuplesR.size()-migrateR;i--){
					tuples=tuples+tuplesR.get(i)+'\n';
				}
				Values value=new Values(SystemParameters.migratedKeys,"R",migrateKey,tuples);
				 collector.emitDirect( dBoltPhysicalIds.get(dest), input,value);
				if(migrateR==tuplesR.size()){
					 R_keyloads.remove(migrateKey);
					 R_tuples.remove(migrateKey);
				}else{
					tuplesR=tuplesR.subList(0, tuplesR.size()-migrateR);
					 R_tuples.put(migrateKey, tuplesR);
					 R_keyloads.put(migrateKey, tuplesR.size());
				}
			}
		}
		Values done=new Values(SystemParameters.completeMigrate,"","","");
		 collector.emitDirect( dBoltPhysicalIds.get(dest),input, done);
		//System.out.println( taskIndex+"号DBolt发送完毕所有需要迁移的tuple");
	}
	
	
	
}

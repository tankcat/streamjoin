package spout.loadbalance;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import redis.clients.jedis.Jedis;
import tool.SystemParameters;

public class LBSpoutForJoin extends BaseRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5606760733155264603L;
	
	private String host;
	private int port;
	private transient Jedis jedis=null;
	private String dataSource;
	private static boolean flag=true;
	
	private SpoutOutputCollector collector;
	private List<Integer> uBoltPhysicalIds;
	
	public LBSpoutForJoin(String host,int port,String dataSource){
		this.host=host;
		this.port=port;
		this.dataSource=dataSource;
	}
 
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		 jedis= getConnectJedis();
		 this.collector=collector;
		 uBoltPhysicalIds=context.getComponentTasks(SystemParameters.uBoltId);
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		if(jedis.exists(dataSource)){
			String line=jedis.lpop(dataSource);
			if(line!=null&&!line.equals("")){
				String rel=(Math.random()>0.5?"R":"S");
				//System.out.println(line);
				Values value=new Values(SystemParameters.tuple,rel,line);
				collector.emit(value,line);
			}
		}else{
			if(flag==true){
				Values values=new Values(SystemParameters.sendEnd);
				for(int i=0;i<uBoltPhysicalIds.size();i++){
					collector.emitDirect(uBoltPhysicalIds.get(i),"sendend",values);
				}
			}
			flag=false;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields(SystemParameters.signal,SystemParameters.relation,SystemParameters.text));
		declarer.declareStream("sendend", new Fields(SystemParameters.signal));
	}
	
	
	
	@Override
	public void close(){
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

}

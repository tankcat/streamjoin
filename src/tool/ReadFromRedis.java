package tool;


import java.io.BufferedWriter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import redis.clients.jedis.Jedis;
import util.*;
import util.SystemParameters;
import util.mfm.MigrationInfo;

public class ReadFromRedis {

	private String host="127.0.0.1";
	private int port=6379;
	private transient Jedis jedis=null;
	
	private Jedis getConnectedJedis(){
		if(jedis!=null){
			return jedis;
		}else{
			jedis=new Jedis(host,port);
		}
		return jedis;
	}
	
	private void disconnectJedis(){
		if(jedis!=null){
			jedis.disconnect();
			jedis=null;
		}
	}
	
	public void readTable(String dataSource){
		jedis=this.getConnectedJedis();
		BufferedWriter bw=FileProcess.getWriter("Experiment/table.txt");
		FileProcess.write("路由表", bw);
		if(jedis.exists(dataSource.getBytes())){
			byte[] bytes=jedis.get(dataSource.getBytes());
			Object obj=SerializeUtil.unserialize(bytes);
			if(obj!=null){
				HashMap<Integer,List<RouteTableItem>> routingTable=(HashMap<Integer,List<RouteTableItem>>)obj;
				Iterator<Entry<Integer, List<RouteTableItem>>> iter=routingTable.entrySet().iterator();
				while(iter.hasNext()){
					Map.Entry<Integer, List<RouteTableItem>> entry=iter.next();
					String str="key = "+entry.getKey()+" ";
					for(int i=0;i<entry.getValue().size();i++){
						str=str+("["+entry.getValue().get(i).toString()+"] ");
					}
					FileProcess.write(str,bw);
				}
			}
		}
		jedis.disconnect();
	}
	
	@SuppressWarnings("unchecked")
	public void readPlans(String dataSource){
		jedis=this.getConnectedJedis();

		if(jedis.exists(dataSource.getBytes())){
			for(int i=0;i<jedis.llen(dataSource.getBytes());i++) {
				BufferedWriter bw= FileProcess.getWriter("Experiment/plan"+i+".txt");
				byte[] bytes = jedis.lindex(dataSource.getBytes(),jedis.llen(dataSource.getBytes())-1-i);
				Object obj = SerializeUtil.unserialize(bytes);
				if (obj != null) {
					HashMap<Integer, MigrationInfo> plan = (HashMap<Integer, MigrationInfo>) obj;
					Iterator<Map.Entry<Integer, MigrationInfo>> iter = plan.entrySet().iterator();
					while (iter.hasNext()) {
						Map.Entry<Integer, MigrationInfo> item = iter.next();
						item.getValue().selfindex=item.getKey();
						item.getValue().getMigrateVolume();
						HashMap<Integer, Double[]> migInfo = item.getValue().mig_info;
						for (Map.Entry<Integer, Double[]> migInfoItem : migInfo.entrySet()) {
							double R=(migInfoItem.getValue()[0]==null?0.0:migInfoItem.getValue()[0]);
							double S=(migInfoItem.getValue()[1]==null?0.0:migInfoItem.getValue()[1]);
							FileProcess.write(item.getKey() + " " + migInfoItem.getKey() + " " + R + " " + S, bw);
						}
					}
				}
				try {
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
		jedis.disconnect();
	}
	
	public void readLineItems(String dataSource){
		jedis=getConnectedJedis();
		if(jedis.exists(dataSource)){
			int index=0;
			while(index<jedis.llen(dataSource)){
				System.out.println(jedis.lindex(dataSource, index));
			}
		}
	}
	
	public void readIndex(String dataSource){
		jedis=getConnectedJedis();
		byte[] bytes=jedis.get(dataSource.getBytes());
		Object obj=SerializeUtil.unserialize(bytes);
		if(obj!=null){
			List<TableItemForJoin> list=(List<TableItemForJoin>)obj;
			for(int i=0;i<list.size();i++){
				System.out.print(list.get(i));
			}
		}
		bytes=jedis.get((dataSource+"2").getBytes());
		obj=SerializeUtil.unserialize(bytes);
		if(obj!=null){
			List<Integer> temp=(List<Integer>)obj;
			for(int i=0;i<temp.size();i++){
				System.out.print(temp.get(i)+" ");
			}
		}
	}
	
	public void readExperimentResults(){
		jedis=getConnectedJedis();
		BufferedWriter bw=FileProcess.getWriter("Experiment/expResults.txt");
		FileProcess.write("路由表", bw);
		for(int i=0;i<jedis.llen("routeTableSize");i++){
			String item=jedis.lindex("routeTableSize", i);
			String[] size=item.split("-");
			FileProcess.write("第"+size[0]+"次制定路由表\t"+size[1], bw);
 		}
		FileProcess.write("\n", bw);
		FileProcess.write("迁移计划制定时常", bw);
		for(int i=0;i<jedis.llen("migrateTime");i++){
			String item=jedis.lindex("migrateTime", i);
			String[] time=item.split("-");
			FileProcess.write("第"+time[0]+"次制定迁移计划\t"+time[1]+"ms", bw);
		}
		FileProcess.write("\n", bw);
		FileProcess.write("迁移量", bw);
		for(int i=0;i<jedis.llen("migrateCount");i++){
			String item=jedis.lindex("migrateCount", i);
			String[] count=item.split("-");
			FileProcess.write("第"+count[0]+"次制定迁移计划\t"+count[1]+"ms", bw);
		}
		FileProcess.write("\n", bw);
		FileProcess.write("广播量", bw);
		for(int i=0;i<jedis.llen("broadcast");i++){
			String item=jedis.lindex("broadcast", i);
			String[] bc=item.split("-");
			FileProcess.write("第"+bc[0]+"个UBolt的广播量\t"+bc[1]+"ms", bw);
		}


		System.out.println("done");
	}


	public void readKeyDetail(){
		BufferedWriter bw=FileProcess.getWriter("Experiment/details.txt");
		for(int i=0;i< SystemParameters.DBOLT_PARA;i++){
			byte[] bytes=jedis.get(("detail"+i).getBytes());
			Object obj=SerializeUtil.unserialize(bytes);
			if(obj!=null){
				HashMap<Integer,Integer> keyDetails=(HashMap<Integer,Integer>)obj;
				FileProcess.write("第"+i+"号DBolt的负载汇报:",bw);
				Iterator iterator=keyDetails.entrySet().iterator();
				while(iterator.hasNext()){
					Map.Entry<Integer,Integer> entry=(Map.Entry<Integer,Integer>)iterator.next();
					int key=entry.getKey();
					int value=entry.getValue();
					FileProcess.write("key = "+key+",count = "+value,bw);
				}
			}
			FileProcess.write("",bw);
		}
	}
	public static void main(String[] args){
		ReadFromRedis rfj=new ReadFromRedis();
		//rfj.readExperimentResults();
		//rfj.readIndex("wrongindex");
		//rfj.readTable("routetable");
		rfj.readPlans("planseri");
		//rfj.readKeyDetail();
		
	}

}

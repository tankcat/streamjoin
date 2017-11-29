package bolt.mfm;

import java.io.BufferedWriter;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import bolt.mfm.migrationplan.*;
import tool.FileProcess;
import tool.SystemParameters;
import util.mfm.TupleProperties;
import util.mfm.ExperimentInfo;
import util.mfm.Mapping;
import util.mfm.MigrationInfo;
import util.mfm.SendInfo;
import tool.SerializeUtil;
import util.mfm.Systemparameters;
import util.mfm.ThetaState.state;

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
public class ControllerMFM extends BaseRichBolt {
	
	private static final long serialVersionUID = -3032249550113230965L;
	private state tickState;
	private int emitFrequency;
	private OutputCollector collector;
	private state ctrlState;
	private int curEpochNumber;
	private double totalFirstRelation;
	private double totalSecondRelation;
	private int currentNumberOfReportedJoiners=0;
	private int usedJoinerNumber;
	private transient Jedis jedis=null;

	private String joinerID;
	private List<Integer> taskPhysicalIDs;
	private List<Integer> taskLogicalIDs;

	private  String host="127.0.0.1";
	private  int port=6379;
	private int time=0;

	private Mapping oldMap;
	private Mapping newMap;
	
	private List<Integer> usedJoiner;
	private Set<Integer> rowJoiner;
	private Set<Integer> colJoiner;
	
	private Map<Integer,Integer> oldLogical;
	private Map<Integer,Integer> newLogical;
	private List<Integer> sendList;
	
	
	private List<Integer> newList;
	private int currentNumberOfAckedNewNode;
	private int currentNumber=0;
	
	private int curRow=1;
	private int curCol=1;

	
	private Map<Integer,Integer> migTo;
	
	private long migBegin,migEnd;

	private int k=0;

	private BufferedWriter bw;
	
	public ControllerMFM(String host,int port,int emitFrequency,String joinerID,int initialUsedJoiner){
		this.emitFrequency=emitFrequency;
		this.joinerID=joinerID;
		this.usedJoinerNumber=initialUsedJoiner;
		this.curCol=1;
		this.curRow=1;
		this.host=host;
		this.port=port;
	}

	@Override
	public void execute(Tuple tupleRecv) {

		if(isTickTuple(tupleRecv)&&ctrlState==state.NORMAL&&tickState==state.NORMAL){
			emitGetStatisticsSignal();
		}else{
			String inputStream=tupleRecv.getSourceStreamId();
			if(inputStream.equals(Systemparameters.ThetaJoinerSignal)){
				String signal=tupleRecv.getStringByField(TupleProperties.MESSAGE);
				if(signal.equals(Systemparameters.DataMigrationEnded)){
					System.out.println("收到节点迁移结束信号");
					processMigrationEnded(tupleRecv);
				}else if(signal.equals(Systemparameters.DtatMigrationFinalize)){
					processMigrationFinalize();
				}
				else{
					//收到的信息是|R|-|S|
					String[] cardinality=signal.split("-");
					int taskIndex=taskPhysicalIDs.indexOf(tupleRecv.getSourceTask());
					System.out.println("controller收到"+taskIndex+"号汇报的负载");
					getStatisitics(Double.parseDouble(cardinality[0]),Double.parseDouble(cardinality[1]),taskIndex);
					if(currentNumberOfReportedJoiners==rowJoiner.size()+colJoiner.size()){
						currentNumberOfReportedJoiners=0;
						tickState=state.NORMAL;
						double load=totalFirstRelation/curRow+totalSecondRelation/curCol;
						k++;
						jedis=this.getConnectedJedis();
						System.out.println(k+"\t\t"+(totalFirstRelation/curRow)+"\t\t"+(totalSecondRelation/curCol)+"\t\t"+load+"\t\t"+Para.V);
						//jedis.lpush("statistics", k+"\t\t"+(totalFirstRelation/curRow)+"\t\t"+(totalSecondRelation/curCol)+"\t\t"+load+"\t\t"+Para.V);
						if(load>Para.V*0.8){
							//jedis.lpush("statistics", k+"\t\t"+(totalFirstRelation/curRow)+"\t\t"+(totalSecondRelation/curCol)+"\t\t"+load+"\t\t"+Para.V);
							Para.newR=totalFirstRelation;
							Para.newS=totalSecondRelation;
							ctrlState=state.DATAMIGRATION;
							long beginTime=System.currentTimeMillis();
							Do_optimal.get_row_col(Para.oldR,Para.oldS,Para.V);
							Para.oldnodes_optimal = new old_Node_optimal[Para.mr_row][Para.ns_col];
							Do_optimal.initialize_nodes(Para.oldnodes_optimal,Para.oldR,Para.oldS,Para.mr_row,Para.ns_col,Para.last_num,Para.lack_row_or_col, Para.V);
							
							oldMap=getOldMapping();
							
							Do_optimal.get_row_col(Para.newR,Para.newS,Para.V);
							
							boolean isUpRight=(Para.lack_row_or_col.equals("col")?true:false);
							
							Para.newnodes_optimal = new new_Node_optimal[Para.mr_row][Para.ns_col];
							Do_optimal.initialize_new_nodes(Para.newnodes_optimal,Para.newR,Para.newS,Para.mr_row,Para.ns_col,Para.last_num,Para.lack_row_or_col, Para.V);
							
							Do2_optimal.make_map(Para.oldnodes_optimal,Para.newnodes_optimal);
							
							Do3_optimal.gen_migration_plan(Para.relationList_optimal);
							
							long endTime=System.currentTimeMillis();
							Do4.Merge_migration_plan(Para.migrationList);
							
							
							long gap=endTime-beginTime;
				
							//获得行数和列数
							int[] rc=getRowandCol(Para.migrationList);
							curRow=rc[0];
							curCol=rc[1];
							newMap=getNewMapping(oldMap,curRow,curCol);
							getNewLogical(curRow,curCol);
							//printMatrix(newMap.index);
							getNewList(Para.migrationList);
							
							
							//将计划写入redis
							int discard=curRow*curCol-newMap.nodeCount;
							
							
							usedJoiner=new ArrayList<Integer>();
							usedJoiner.addAll(newLogical.values());
							usedJoinerNumber=usedJoiner.size();
							resetSetJoiner();
							int targetIndex=0;
							MigrationInfo mInfo=null;
							Map<Integer,MigrationInfo> plan=new HashMap<Integer,MigrationInfo>();
							for(int i=0;i<Para.migrationList.size();i++){
								//如果是新增节点，则不用发送迁移计划
								Mig_Info item=Para.migrationList.get(i);
								if(item.getI_old()==65535){
									mInfo=new MigrationInfo();
									mInfo.isNewNode=true;
									mInfo.isActive=true;
									targetIndex=getTargetIndex(item.getI_new_from(),item.getJ_new_from(),false);
								}
								else{
									targetIndex=getTargetIndex(item.getI_old(),item.getJ_old(),true);
									old_Node_optimal node=Para.oldnodes_optimal[item.getI_old()][item.getJ_old()];
									double beginR=node.getR_begin();
									double endR=node.getR_end();
									double beginS=node.getS_begin();
									double endS=node.getS_end();
									mInfo=new MigrationInfo();
									mInfo.isNewNode=false;
									mInfo.isActive=true;
									Vector<To_I_J_B_E> r_send=item.getR_i_j_list();
									Vector<To_I_J_B_E> s_send=item.getS_i_j_list();
									Vector<SendInfo> r_send_info=null;
									Vector<SendInfo> s_send_info=null;
									if(item.getI_new_from()!=65535){
										r_send_info=fooSendInfo(r_send,beginR,endR);
										s_send_info=fooSendInfo(s_send,beginS,endS);
									}
									mInfo.r_discard=fooDiscardInfo(item.getR_discard_list(),beginR,endR);
									mInfo.s_discard=fooDiscardInfo(item.getS_discard_list(),beginS,endS);
									mInfo.r_send=r_send_info;
									mInfo.s_send=s_send_info;
									if(item.getI_new_from()==65535)
										mInfo.isActive=false;
								}
								plan.put(targetIndex, mInfo);
							}
							bw=FileProcess.getWriter("Experiment/plan"+curEpochNumber+".txt");
							recordPlanToFiles(bw,plan);
							getMigrateToSet(Para.migrationList);
							ExperimentInfo ei=new ExperimentInfo(gap,totalFirstRelation,totalSecondRelation,usedJoinerNumber);
							writeNewSchemeToJedis(newMap,curRow,curCol,discard,isUpRight,plan,ei);
							//Tools.printmigrationplan(Para.migrationList,bwplan);
							Para.oldR=Para.newR;
							Para.oldS=Para.newS;
							oldLogical=newLogical;
							newLogical=new HashMap<Integer,Integer>();
							getSendList();
							Para.migrationList =new Vector<Mig_Info>();
							Para.relationList_optimal =new Vector<Old_New_Relation_optimal>();
						}
						totalFirstRelation=0;
						totalSecondRelation=0;
					}
				}
			}
		}
		
	}

	private void recordPlanToFiles(BufferedWriter bw,Map<Integer,MigrationInfo> plan){
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
		FileProcess.close(bw);
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector=collector;
		jedis=getConnectedJedis();
		ctrlState=state.NORMAL;
		tickState=state.NORMAL;
		curEpochNumber=0;
		taskPhysicalIDs=context.getComponentTasks(joinerID);
		taskLogicalIDs=new ArrayList<Integer>();
		for(int i=0;i<taskPhysicalIDs.size();i++)
			taskLogicalIDs.add(i);
		usedJoiner=new ArrayList<Integer>();
		usedJoiner.add(0);
		rowJoiner=new HashSet<Integer>();
		rowJoiner.add(0);
		colJoiner=new HashSet<Integer>();
		colJoiner.add(0);
		oldLogical=new HashMap<Integer,Integer>();
		newLogical=new HashMap<Integer,Integer>();
		oldLogical.put(0, 0);
		sendList=new ArrayList<Integer>();
		sendList.add(0);
		currentNumberOfAckedNewNode=0;
		
		migTo=new HashMap<Integer,Integer>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declareStream(Systemparameters.ThetaControllerSignal, new Fields(TupleProperties.MESSAGE,TupleProperties.TICKTIME));
		
	}
	
	@Override
	public Map<String,Object> getComponentConfiguration(){
		Map<String,Object> conf=new HashMap<String,Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, this.emitFrequency);
		return conf;
	}
	
	
	
	private boolean isTickTuple(Tuple tuple){
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)&&
				tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
	
	private void emitGetStatisticsSignal(){
		//给当前正在运行中的joiner发送Tick tuple
		tickState=state.TICK;
		time++;
		String str="";
		System.out.println("controller第"+time+"次发送tick");
		for(int i=0;i<usedJoiner.size();i++){
			str=str+i+" ";
			/*Values value=new Values("N/A",time);
			collector.emitDirect(taskPhysicalIDs.get(usedJoiner.get(i)),Systemparameters.ThetaControllerSignal,tuple,value);*/
		}
		jedis.set("usedJoiner", str);
		jedis.set("ticktime", time+"");
		jedis.lpush("tick", "controller第"+time+"发送Tick");
	}
	
	
	private void processMigrationEnded(Tuple tupleRecv){
		currentNumberOfAckedNewNode++;
		//int sourceID=taskPhysicalIDs.indexOf(tupleRecv.getSourceTask());
		//jedis.lpush(curEpochNumber+"-getend",sourceID+"");
		if(currentNumberOfAckedNewNode==migTo.size()){
			migEnd=System.currentTimeMillis();
			long gap=migEnd-migBegin;
			jedis.lpush("migTime",gap+","+curEpochNumber);
			currentNumberOfAckedNewNode=0;
			migTo=new HashMap<Integer,Integer>();
			for(int i=0;i<usedJoiner.size();i++){
				Values value=new Values(Systemparameters.DataMigrationEnded,-1);
				collector.emitDirect(taskPhysicalIDs.get(usedJoiner.get(i)), Systemparameters.ThetaControllerSignal,tupleRecv,value);
			}
			System.out.println("所有的joiner迁移完毕");
		}
	}
	
	private void processMigrationFinalize(){
		this.currentNumber++;
		System.out.println("finalize个数"+this.currentNumber+"/"+usedJoinerNumber);
		if(this.currentNumber==usedJoinerNumber){
			currentNumber=0;
			ctrlState=state.NORMAL;
			jedis=this.getConnectedJedis();
			if(jedis.exists("plan".getBytes()))
				jedis.del("plan".getBytes());
			if(jedis.exists("migTo".getBytes()))
				jedis.del("migTo".getBytes());
			//System.out.println("���е�JoinerǨ�����");
		}
	}
	
	private Jedis getConnectedJedis(){
		if(jedis!=null){
			return jedis;
		}else{
			jedis=new Jedis(host,port);
		}
		return jedis;
	}

	private void writeNewSchemeToJedis(Mapping newMap,int row,int col,int discard,boolean isUpRight,Map<Integer,MigrationInfo> plan,ExperimentInfo ei){
		bw=FileProcess.getWriter("Experiment/matrix"+curEpochNumber+".txt");
		jedis=this.getConnectedJedis();
		String newSchemeInfo=row+" "+col+" "+newMap.nodeCount+" "+discard+" "+(isUpRight==true?1:0);
		String matrix="";
		String tmp="";
		for(int i=0;i<row;i++){
			for(int j=0;j<col;j++){
				if(newLogical.containsKey(newMap.index[i][j])){
					matrix+=newLogical.get(newMap.index[i][j])+" ";
					tmp+=newLogical.get(newMap.index[i][j])+" ";
				}else{
					matrix+=-1+" ";
					tmp+=-1+" ";
				}
			}
			FileProcess.write(tmp,bw);
			tmp="";
			matrix+='|';
		}
		jedis.set("plan".getBytes(), SerializeUtil.serialize(plan));
		//System.out.println(matrix);
		//jedis.lpush("planseri".getBytes(),SerializeUtil.serialize(plan));
		jedis.set("migTo".getBytes(), SerializeUtil.serialize(migTo));
		jedis.set("newMap", matrix);
		jedis.set("newSchemeInfo", newSchemeInfo);
		
		jedis.lpush("expinfo".getBytes(),SerializeUtil.serialize(ei));
		jedis.lpush("matrix", matrix);
		curEpochNumber++;
		jedis.set("curEpochNumber", curEpochNumber+"");
		migBegin=System.currentTimeMillis();
		FileProcess.close(bw);
	}
	
	@Override
	public void cleanup(){
		//disconnect();
	}

	private Mapping getNewMapping(Mapping oldMap,int row,int col){
		int count=0;
		Mapping newMapping=new Mapping();
		newMapping.mapping=new int[row][col];
		newMapping.index=new int[row][col];
		int k=0;
		for(int i=0;i<row;i++){
			for(int j=0;j<col;j++){
				newMapping.mapping[i][j]=-1;
				if(Para.newnodes_optimal[i][j].isActivity())
					newMapping.index[i][j]=k++;
				else
					newMapping.index[i][j]=-1;
			}
		}
		int oldCount=oldMap.nodeCount;
		for(int i=0;i<Para.migrationList.size();i++){
			int ith=Para.migrationList.get(i).getI_old();
			int jth=Para.migrationList.get(i).getJ_old();
			int _ith=Para.migrationList.get(i).getI_new_from();
			int _jth=Para.migrationList.get(i).getJ_new_from();
			if(ith!=65535&&_ith!=65535){
				newMapping.mapping[_ith][_jth]=oldMap.mapping[ith][jth];
				count++;
			}
			if(ith==65535&&_ith!=65535){
				newMapping.mapping[_ith][_jth]=oldCount++;
				count++;
			}
		}
		newMapping.nodeCount=count;
		return newMapping;
	}
	
	private  Mapping getOldMapping(){
		Mapping oldMap=new Mapping();
		oldMap.mapping=new int[Para.mr_row][Para.ns_col];
		int k=0;
		for(int i=0;i<Para.mr_row;i++){
			for(int j=0;j<Para.ns_col;j++){
				if(Para.oldnodes_optimal[i][j].isActivity()){
					oldMap.mapping[i][j]=k++;
				}else{
					oldMap.mapping[i][j]=-1;
				}
			}
		}
		oldMap.nodeCount=k;
		return oldMap;
	}
	
	
	private int getTargetIndex(int ith,int jth,boolean toOld){
		int result=0;
		if(toOld){
			result=oldLogical.get(oldMap.mapping[ith][jth]);
		}else{
			result=newLogical.get(newMap.index[ith][jth]);
		}
		return result;
	}
	
	private Vector<SendInfo> fooSendInfo(Vector<To_I_J_B_E> sendInfo,double oldbegin,double oldend){
		Vector<SendInfo> result=new Vector<SendInfo>();
		for(int i=0;i<sendInfo.size();i++){
			SendInfo temp=new SendInfo();
			To_I_J_B_E item=sendInfo.get(i);
			temp.targetIndex=getTargetIndex(item.getI_new(),item.getJ_new(),false);
			temp.be_send_list=fooDiscardInfo(item.getBe_send_list(),oldbegin,oldend);
			result.addElement(temp);
		}
		return result;
	}
	
	private Vector<Begin_End> fooDiscardInfo(Vector<Begin_End> list,double begin,double end){
		Vector<Begin_End> result=new Vector<Begin_End>();
		for(int i=0;i<list.size();i++){
			if(list.get(i).getBegin()==list.get(i).getEnd())
				continue;
			Begin_End e=editBeginAndEnd(begin,end,list.get(i).getBegin(),list.get(i).getEnd());
			e.setUnregular(list.get(i).isUnregular());
			result.add(e);	
		}
		return result;
	}
	
	private void resetSetJoiner(){
		rowJoiner=new HashSet<Integer>();
		colJoiner=new HashSet<Integer>();
		for(int i=0;i<newMap.mapping.length;i++){
			rowJoiner.add(newLogical.get(newMap.index[i][0]));
		}
		for(int i=0;i<newMap.mapping[0].length;i++){
			colJoiner.add(newLogical.get(newMap.index[0][i]));
		}
	}
	
	private void getStatisitics(double R,double S,int taskIndex){
		if(rowJoiner.contains(taskIndex)){
			//jedis.lpush(this.time+"-getreport", taskIndex+"-R");
			totalFirstRelation+=R;
			currentNumberOfReportedJoiners++;
		}
		if(colJoiner.contains(taskIndex)){
			//jedis.lpush(this.time+"-getreport", taskIndex+"-S");
			totalSecondRelation+=S;
			currentNumberOfReportedJoiners++;
		}
	}
	
	private void printMatrix(int[][] matrix){
		for(int i=0;i<matrix.length;i++){
			for(int j=0;j<matrix[i].length;j++){
				//System.out.print(newLogical.get(matrix[i][j])+" ");
			}
			//System.out.println();
		}
	}
	

	private void getNewLogical(int row,int col){
		Set<Integer> all=new HashSet<Integer>(taskLogicalIDs);
		all.removeAll(usedJoiner);
		Iterator<Integer> iter=all.iterator();
		for(int i=0;i<row;i++){
			for(int j=0;j<col;j++){
				if(newMap.mapping[i][j]!=-1){
					if(oldLogical.containsKey(newMap.mapping[i][j])){
						Integer v=oldLogical.get(newMap.mapping[i][j]);
						newLogical.put(newMap.index[i][j], v);
					}else{
						if(iter.hasNext())
							newLogical.put(newMap.index[i][j],iter.next());
						else{
							
						}
					}
				}	
			}
		}
	}
	
	private void getSendList(){
		sendList=new ArrayList<Integer>();
		Set<Integer> temp=new HashSet<Integer>();
		temp.addAll(rowJoiner);
		temp.addAll(colJoiner);
		sendList.addAll(temp);
	}
	
	
	private Begin_End editBeginAndEnd(double oldbegin,double oldend, double newbegin,double newend){
		Begin_End e=new Begin_End();
		e.setBegin((newbegin-oldbegin)/(oldend-oldbegin));
		e.setEnd((newend-oldbegin)/(oldend-oldbegin));
		return e;
	}
	
	private int[] getRowandCol(Vector<Mig_Info> list){
		int[] result=new int[2];
		List<Integer> row=new ArrayList<Integer>();
		List<Integer> col=new ArrayList<Integer>();
		for(int i=0;i<list.size();i++){
			if(list.get(i).getI_new_from()!=65535){
				row.add(list.get(i).getI_new_from());
				col.add(list.get(i).getJ_new_from());
			}
		}
		result[0]=Collections.max(row)+1;
		result[1]=Collections.max(col)+1;
		return result;
	}
	
	private void getNewList(Vector<Mig_Info> list){
		newList=new ArrayList<Integer>();
		int temp=0;
		for(int i=0;i<list.size();i++){
			if(list.get(i).getI_old()==65535){
				temp=getTargetIndex(list.get(i).getI_new_from(),list.get(i).getJ_new_from(),false);
				newList.add(temp);
			}
		}
	}
	
	private void getMigrateToSet(Vector<Mig_Info> list){
		int temp=0;
		for(int i=0;i<list.size();i++){
			if(list.get(i).getI_old()!=65535){
				Vector<To_I_J_B_E> toR=list.get(i).getR_i_j_list();
				for(int j=0;j<toR.size();j++){
					if(toR.get(j).getBe_send_list().size()>0){
						temp=getTargetIndex(toR.get(j).getI_new(),toR.get(j).getJ_new(),false);
						Vector<Begin_End> sendlist=toR.get(j).getBe_send_list();
						for(int k=0;k<sendlist.size();k++){
							Begin_End item=sendlist.get(k);
							if(item.getBegin()==item.getEnd())
								continue;
							if(migTo.containsKey(temp)){
								int value=migTo.get(temp);
								migTo.put(temp, value+1);
							}else{
								migTo.put(temp, 1);
								
							}
						}
					}
				}
				Vector<To_I_J_B_E> toS=list.get(i).getS_i_j_list();
				for(int j=0;j<toS.size();j++){
					if(toS.get(j).getBe_send_list().size()>0){
						temp=getTargetIndex(toS.get(j).getI_new(),toS.get(j).getJ_new(),false);
						Vector<Begin_End> sendlist=toS.get(j).getBe_send_list();
						for(int k=0;k<sendlist.size();k++){
							Begin_End item=sendlist.get(k);
							if(item.getBegin()==item.getEnd())
								continue;
							if(migTo.containsKey(temp)){
								int value=migTo.get(temp);
								migTo.put(temp, value+1);
							}else{
								migTo.put(temp, 1);
							}
						}	
					}
				}
			}
		}
		System.out.println(migTo);
	}
}

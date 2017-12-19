package bolt.mfm;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;

import util.mfm.TupleProperties;
import util.mfm.Quadruple;
import util.mfm.SendInfo;
import tool.SerializeUtil;
import util.mfm.MigVolume;
import util.mfm.MigrationInfo;
import util.mfm.MyComparator;
import util.MyCompare;
import util.mfm.MyUtilities;
import util.mfm.Systemparameters;
import util.mfm.ThetaState.state;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import bolt.mfm.migrationplan.Begin_End;
import redis.clients.jedis.Jedis;

public class JoinMFM extends BaseRichBolt {
	private int curEpochNumber=0;
	private OutputCollector _collector;
	private TopologyContext _context;
	private static final long serialVersionUID = 7763094959043248256L;
	
	private String controllerID;
	private int controllerIndex;
	private state joinerState;
	
	private TreeMap<Integer,String> R,S;
	private TreeMap<Integer,String> RTag,STag;
	private TreeMap<Integer,String> RNew,SNew;
	private TreeMap<Integer,String> RSpecial,SSpecial;
	private TreeMap<Integer,String> RTagSpecial,STagSpecial;
	
	private transient Jedis jedis=null;
	private String host;
	private int port;
	
	private int taskIndex;
	private List<Integer> taskPhysicalIDs;

	
	private int currentNumberOfSource;
	private int sourceCount;
	
	String schemeInfo="";
	String joinerID="";
	
	
	public JoinMFM(String host,int port){
		this.host=host;
		this.port=port;
	}
	
	public Jedis getJedis(){
		jedis=getConnectedJedis();
		return jedis;
	}

	/**
	 *
	 * @param host redis主机ip
	 * @param port redis端口
	 * @param controllerID controllerBolt的id
	 * @param joinerID joinerBolt的
	 */
	public JoinMFM(String host,int port,String controllerID,String joinerID) {
		// TODO Auto-generated constructor stub
		this.host=host;
		this.port=port;
		this.controllerID=controllerID;
		this.joinerID=joinerID;

		constructStorage();
	}


	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		curEpochNumber=0;
		jedis=getConnectedJedis();
		_collector=collector;
		_context=context;
		joinerState=state.NORMAL;
		controllerIndex=_context.getComponentTasks(this.controllerID).get(0);
		taskIndex=context.getThisTaskIndex();
		taskPhysicalIDs=context.getComponentTasks(joinerID);
		currentNumberOfSource=0;
		schemeInfo=jedis.get("newSchemeInfo");
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple tupleRecv){
		String inputStream=tupleRecv.getSourceStreamId();
		// 数据来自信号流-控制流
		if(inputStream.equals(Systemparameters.ThetaControllerSignal)){
			//System.out.println("收到controller信号");
			String message=tupleRecv.getStringByField(TupleProperties.MESSAGE);
			if(message.equals(Systemparameters.DataMigrationEnded)){
				processMigrationEnded();
			}else{
				reportStatistics();
			}
			return;
		}
		// 数据来自信号流-结束流
		else if(inputStream.equals(Systemparameters.ThetaDataSendEOF)){
			this.currentNumberOfSource++;
			byte[] values=jedis.get("migTo".getBytes());
			Object oj=SerializeUtil.unserialize(values);
			if(oj!=null){
				HashMap<Integer,Integer> migTo=(HashMap<Integer,Integer>)oj;
				if(migTo.containsKey(taskIndex)){
					sourceCount=migTo.get(taskIndex);
				}
			}
			if(this.currentNumberOfSource==this.sourceCount){
				this.currentNumberOfSource=0;
				processMigrationEOF();
			}
			return;
		}
		// 数据来自数据流
		int emitterIndex=tupleRecv.getIntegerByField(TupleProperties.COPM_INDEX);
		boolean isFirst=(emitterIndex==1?true:false);
		// 迁移的数据
		if(inputStream.equals(Systemparameters.ThetaDataMigration)){
			byte[] toSendBytes=tupleRecv.getBinaryByField(TupleProperties.TUPLE);
			Object obj=SerializeUtil.unserialize(toSendBytes);
			if(obj!=null){
				List<String> toSend=(List<String>) obj;
				boolean isUnRegular=tupleRecv.getBooleanByField(TupleProperties.REGULAR);
				transferTagged(toSend, isFirst,isUnRegular);
			}
			return;
		}
		// 原始数据
		else{
			List<String> tuple=(List<String>)tupleRecv.getValueByField(TupleProperties.TUPLE);
			boolean isSpecial=tupleRecv.getBooleanByField(TupleProperties.SPECIAL);
			Integer key=Integer.parseInt(tuple.get(0));
			String inputString=MyUtilities.tupleListToString(tuple);
			Quadruple updateInfo=extractUpdateInfo(inputString,joinerState,isFirst,isSpecial);
			updateInfo.affected.put(key, inputString);
			_collector.ack(tupleRecv);
		}
		if(inputStream.equals(Systemparameters.DATA_STREAM)){
			int tupleEpoch=tupleRecv.getIntegerByField(TupleProperties.EPOCH);
			if(tupleEpoch>curEpochNumber){
				schemeInfo=jedis.get("newSchemeInfo");
				joinerState=state.DATAMIGRATION;
				curEpochNumber=tupleEpoch;
				byte[] value=jedis.get("plan".getBytes());
				byte[] value2=jedis.get("migTo".getBytes());
				if(value==null){
					return;
				}
				if(value2==null){
					return;
				}
				Object object=SerializeUtil.unserialize(value);
				Object object2=SerializeUtil.unserialize(value2);
				this.RSpecial=new TreeMap<>(new MyComparator());
				this.SSpecial=new TreeMap<>(new MyComparator());
				this.RTagSpecial=new TreeMap<>(new MyComparator());
				this.STagSpecial=new TreeMap<>(new MyComparator());
				if(object2!=null){
					HashMap<Integer,Integer> migTo=(HashMap<Integer,Integer>)object2;
					System.out.println("migTo:"+migTo.toString());
					if(migTo.containsKey(taskIndex)){
						sourceCount=migTo.get(taskIndex);
					}
				}	
				MigrationInfo mInfo=null;
				if(object!=null){
					HashMap<Integer,MigrationInfo> plan=(HashMap<Integer,MigrationInfo>)object;
					mInfo=plan.get(taskIndex);
					if(mInfo==null){
						return;
					}
					performDiscardAndDistribute(mInfo,tupleRecv);
				}
			}
		}
	}
	
	private void processMigrationEnded(){
		joinerState=state.NORMAL;
		addTuples(RTag,R);
		addTuples(RNew,R);
		addTuples(STag,S);
		addTuples(SNew,S);
		RTag=new TreeMap<>(new MyComparator());
		STag=new TreeMap<>(new MyComparator());
		RNew=new TreeMap<>(new MyComparator());
		SNew=new TreeMap<>(new MyComparator());
		Values value=new Values(Systemparameters.DtatMigrationFinalize);
		_collector.emitDirect(controllerIndex, Systemparameters.ThetaJoinerSignal,value);
	}
	
	private void processMigrationEOF(){
		Values value=new Values(Systemparameters.DataMigrationEnded);
		_collector.emitDirect(controllerIndex, Systemparameters.ThetaJoinerSignal,value);
	}
	
	private void reportStatistics(){
		 double totalR=R.size()+RSpecial.size()+RTagSpecial.size();
		 double totalS=S.size()+SSpecial.size()+STagSpecial.size();
		 String message=totalR+"-"+totalS;
		 //System.out.println(taskIndex+"号joiner汇报"+message);
		 _collector.emitDirect(controllerIndex, Systemparameters.ThetaJoinerSignal,new Values(message));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		List<String> statisticsFields=new ArrayList<String>();
		statisticsFields.add(TupleProperties.MESSAGE);
		declarer.declareStream(Systemparameters.ThetaJoinerSignal, new Fields(statisticsFields));

		List<String> migrationFields=new ArrayList<String>();
		migrationFields.add(TupleProperties.COPM_INDEX);
		migrationFields.add(TupleProperties.TUPLE);
		migrationFields.add(TupleProperties.REGULAR);
		migrationFields.add(TupleProperties.DELNUM);
		declarer.declareStream(Systemparameters.ThetaDataMigration,new Fields(migrationFields));
		
		declarer.declareStream(Systemparameters.ThetaDataSendEOF,new Fields(TupleProperties.MESSAGE));
	}

	
	private Jedis getConnectedJedis(){
		if(jedis!=null)
			return jedis;
		else
			jedis=new Jedis(this.host,this.port);
		return jedis;
	}

	/**
	 * 初始化各个存储部件
	 */
	private void constructStorage(){
		R=new TreeMap<>(new MyComparator());
		S=new TreeMap<>(new MyComparator());
		RTag=new TreeMap<>(new MyComparator());
		STag=new TreeMap<>(new MyComparator());
		RNew=new TreeMap<>(new MyComparator());
		SNew=new TreeMap<>(new MyComparator());
		RSpecial=new TreeMap<>(new MyComparator());
		SSpecial=new TreeMap<>(new MyComparator());
		RTagSpecial=new TreeMap<>(new MyComparator());
		STagSpecial=new TreeMap<>(new MyComparator());
	}
	
	private Quadruple extractUpdateInfo(String str,state joinerState,boolean isFirst,boolean isSpecial){
		Quadruple tupleQuadInfo=null;
			if(isSpecial){
				if(isFirst){
					tupleQuadInfo=new Quadruple(RSpecial,SSpecial);
				}else{
					tupleQuadInfo=new Quadruple(SSpecial,RSpecial);
				}
			}else{
				if(joinerState==state.NORMAL){
					if(isFirst){
						tupleQuadInfo=new Quadruple(R,S);
					}else{
						tupleQuadInfo=new Quadruple(S,R);
					}	
				}else if(joinerState==state.DATAMIGRATION){
					if(isFirst){
						tupleQuadInfo=new Quadruple(RNew,SNew);
					}else{
						tupleQuadInfo=new Quadruple(SNew,RNew);
					}
				}
			}
		
		return tupleQuadInfo;
	}
	
	private void performJoin(List<String> tupleValues,boolean isFirst,TreeMap<Integer,String> toJoin,boolean isMigrate,String info){
			/*if(toJoin==null||toJoin.size()==0)
				return;
			Iterator it=toJoin.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry<Integer, String> entry=(Entry<Integer, String>) it.next();
				String tupleToJoin=entry.getValue();
				List<String> tupleToJoinList=MyUtilities.stringToTuple(tupleToJoin);
				List<String> firstTuple,secondTuple;
				if(isFirst){
					firstTuple=tupleValues;
					secondTuple=tupleToJoinList;
				}else{
					firstTuple=tupleToJoinList;
					secondTuple=tupleValues;
				}
				String custkey=firstTuple.get(0);
				if(custkey.equals(secondTuple.get(1))){
					String output=MyUtilities.tupleListToString(secondTuple);
					output=info+"-"+taskIndex+"+"+isMigrate+"+"+schemeInfo+"+"+output;
				}
			}*/
			
	}
	

	
	private void performDiscardAndDistribute(MigrationInfo mInfo,Tuple tupleRecv){
		if(!mInfo.isNewNode){
			int i,j;
			int sendBegin,sendEnd;
			int rsize=R.size();
			int ssize=S.size();
			
			Vector<SendInfo> r_send=mInfo.r_send;
			Vector<SendInfo> s_send=mInfo.s_send;
			Vector<Begin_End> r_discard=mInfo.r_discard;
			Vector<Begin_End> s_discard=mInfo.s_discard;
			long migR=0;
			long migS=0;
			for(i=0;i<r_send.size();i++){				
				int targetIndex=r_send.get(i).targetIndex;
				Vector<Begin_End> sendList=r_send.get(i).be_send_list;
				if(sendList.size()>0){
					for(j=0;j<sendList.size();j++){	
						if(sendList.get(j).getBegin()==sendList.get(j).getEnd())
							continue;
						sendBegin=(int) (sendList.get(j).getBegin()*rsize);
						sendEnd=(int)(sendList.get(j).getEnd()*rsize);
						migR+=(sendEnd-sendBegin);
						boolean isUnRegular=sendList.get(j).isUnregular();
						performDistribute(true,sendBegin,sendEnd,targetIndex,isUnRegular);
					}
				}
			}
			for(i=0;i<s_send.size();i++){
				int targetIndex=s_send.get(i).targetIndex;
				Vector<Begin_End> sendList=s_send.get(i).be_send_list;
				if(sendList.size()>0){
					for(j=0;j<sendList.size();j++){
						if(sendList.get(j).getBegin()==sendList.get(j).getEnd())
							continue;
						sendBegin=(int) (sendList.get(j).getBegin()*ssize);
						sendEnd=(int) (sendList.get(j).getEnd()*ssize);
						migS+=(sendEnd-sendBegin);
						boolean isUnRegular=sendList.get(j).isUnregular();
						performDistribute(false,sendBegin,sendEnd,targetIndex,isUnRegular);
					}
				}
			}
			MigVolume mv=new MigVolume(this.curEpochNumber,taskIndex,migR,migS);
			jedis=this.getConnectedJedis();
			jedis.lpush("migVolume".getBytes(), SerializeUtil.serialize(mv));
			if(!mInfo.isActive){
				constructStorage();
				joinerState=state.NORMAL;
			}else{
				performDiscard(true,r_discard,rsize);
				performDiscard(false,s_discard,ssize);
			}
		}
	}
	
	private void performDiscard(boolean isFirst,Vector<Begin_End> discardList,int size){
		TreeMap<Integer,String> discard;
		int i=0;
		int begin,end;
		if(isFirst){
			discard=R;
		}else{
			discard=S;
		}
		if(discardList==null||discardList.size()==0){
			return;
		}
		Collections.sort(discardList,new MyCompare());
		for(i=0;i<discardList.size();i++){
			begin=(int) (discardList.get(i).getBegin()*size);
			end=(int)(discardList.get(i).getEnd()*size);
			int index=0;
			Iterator it=discard.entrySet().iterator();
			while(it.hasNext()&&index<begin){
				it.next();
				index++;
			}
			while(it.hasNext()&&index<end){
				it.next();
				it.remove();
				index++;
			}
		}	
	}
	
	private void performDistribute(boolean isFirst,int begin,int end,int targetIndex,boolean isUnRegular){
		TreeMap<Integer,String> send;
		int emitterIndex=1;
		List<String> toSend=new ArrayList<String>();
		if(isFirst){
			send=R;
			emitterIndex=1;
		}else{
			send=S;
			emitterIndex=2;
		}
		
		Iterator it=send.entrySet().iterator();
		int index=0;
		while(it.hasNext()&&index<begin){
			it.next();
			index++;
		}
		
		int buffersize=1000;
		int k=0;
		while(it.hasNext()&&index<end){
			Map.Entry<Integer, String> entry=(Entry<Integer, String>) it.next();
			String value=entry.getValue();
			toSend.add(value);
			index++;
			k++;
			if(k==buffersize){
				Values tuple=new Values(emitterIndex,SerializeUtil.serialize(toSend),isUnRegular,0);
				_collector.emitDirect(taskPhysicalIDs.get(targetIndex), Systemparameters.ThetaDataMigration,tuple);
				k=0;
				toSend=new ArrayList<String>();
			}
		}
		Values value=new Values(emitterIndex,SerializeUtil.serialize(toSend),isUnRegular,0);
		_collector.emitDirect(taskPhysicalIDs.get(targetIndex), Systemparameters.ThetaDataMigration,value);
		
		_collector.emitDirect(taskPhysicalIDs.get(targetIndex), Systemparameters.ThetaDataSendEOF,new Values(Systemparameters.DataMigrationEOF));
	}
	
	
	private void addTuples(TreeMap<Integer,String> from,TreeMap<Integer,String> to){
		Iterator it=from.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry<Integer, String> entry=(Entry<Integer, String>) it.next();
			Integer key=entry.getKey();
			String value=entry.getValue();
			to.put(key, value);
		}
	}
	
	
	@Override
	public void cleanup(){
		//disconnect();
		
	}
	
	private void disconnect(){
		if(jedis!=null){
			jedis.disconnect();
			jedis=null;
		}
	}
	
	private void transferTagged(List<String> toSend,boolean isFirst,boolean isUnRegular){
		if(toSend!=null&&!toSend.equals("")){
			if(isFirst){
				if(!isUnRegular){
					for(int i=0;i<toSend.size();i++){
						List<String> values=MyUtilities.stringToTuple(toSend.get(i));
						Integer key=Integer.parseInt(values.get(0));
						RTag.put(key, toSend.get(i));
						//performJoin(values,true,SNew,false,"tag-SN");
						//performJoin(values,true,SSpecial,false,"tag-SS");
					}
				}else{
					for(int i=0;i<toSend.size();i++){
						List<String> values=MyUtilities.stringToTuple(toSend.get(i));
						Integer key=Integer.parseInt(values.get(0));
						RTagSpecial.put(key, toSend.get(i));
						//performJoin(values,true,SNew,false,"tag-SN");
						//performJoin(values,true,SSpecial,false,"tag-SS");
					}
				}
			}else{
				if(!isUnRegular){
					for(int i=0;i<toSend.size();i++){
						List<String> values=MyUtilities.stringToTuple(toSend.get(i));
						Integer key=Integer.parseInt(values.get(0));
						STag.put(key, toSend.get(i));
						//performJoin(values,false,RNew,false,"tag-RN");
						//performJoin(values,false,RSpecial,false,"tag-RS");
					}
				}else{
					for(int i=0;i<toSend.size();i++){
						List<String> values=MyUtilities.stringToTuple(toSend.get(i));
						Integer key=Integer.parseInt(values.get(0));
						STagSpecial.put(key, toSend.get(i));
						//performJoin(values,false,RNew,false,"tag-RN");
						//performJoin(values,false,RSpecial,false,"tag-RS");
					}
				}
			}
		}
	}
	
	
}

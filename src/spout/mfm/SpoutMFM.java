package spout.mfm;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import util.mfm.AssignNode;
import util.mfm.Matrix;
import util.mfm.TupleProperties;
import util.mfm.Systemparameters;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import redis.clients.jedis.Jedis;

public class SpoutMFM implements IRichSpout {
	private static final long serialVersionUID = 9053071391854003232L;
	private String host;
	private int port;
	private String dataSource;
	private transient Jedis jedis=null;
	private SpoutOutputCollector _collector;
	private int curEpochNumber=0;
	private String joinerID;
	private List<Integer> joinerTaskPhysicalIDs;
	
	private Matrix curMatrix;
	private int[][] map;

	
	private Map<Integer,List<AssignNode>> rowNodes;
	private Map<Integer,List<AssignNode>> colNodes;

	private int ticktime=0;
	private int taskId;

	private int index=0;
	private long totalData=0;
	public SpoutMFM(){

	}

	/**
	 *
	 * @param host redis主机ip
	 * @param port redis端口
	 * @param dataSource 数据源名
	 * @param initialMatrix 初始矩阵
	 * @param joinerID joiner的
	 */
	public SpoutMFM(String host,int port,String dataSource,int[] initialMatrix,String joinerID) {
		// TODO Auto-generated constructor stub
		this.host=host;
		this.port=port;
		this.dataSource=dataSource;
		this.joinerID=joinerID;

		curMatrix=new Matrix(initialMatrix[0],initialMatrix[1],initialMatrix[0]*initialMatrix[1],0,0);
		map=new int[curMatrix.row][curMatrix.col];

		// 给矩阵中每个节点赋予逻辑编号
		int k=0;
		for(int i=0;i<curMatrix.row;i++){
			for(int j=0;j<curMatrix.col;j++){
				map[i][j]=k++;
			}
		}
	}

	@Override
	public void ack(Object arg0) {
		

	}

	@Override
	public void activate() {
		

	}

	@Override
	public void close() {
		
	}

	@Override
	public void deactivate() {
		

	}

	@Override
	public void fail(Object arg0) {
		

	}

	//从redis中获取指定数据源中的数据
	private String getContent(int index){
		if(jedis.exists(dataSource)){
			return jedis.lindex(dataSource,index);
		}
		return null;
	}
	
	@Override
	public void nextTuple(){
		int controllertick = Integer.parseInt(jedis.get("ticktime"));
		//System.out.println(controllertick+","+taskId);
		// 新的tick信号
		if(controllertick > this.ticktime && this.taskId == 0){
			System.out.println("发送tick信号");
			this.ticktime = controllertick;
			String joiners = jedis.get("usedJoiner");
			String[] toSend = joiners.split(" ");
			for(int i = 0; i < toSend.length; i++){
				/*信号流tuple的各个字段：消息类别
				 *                    ticktime
				 */
				Values value=new Values("sendtick",this.ticktime);
				int dest = Integer.parseInt(toSend[i]);
				_collector.emitDirect(joinerTaskPhysicalIDs.get(dest),Systemparameters.ThetaControllerSignal,value);
			}
			return;
		}
			try {
				int index=0;
				if(index<totalData) {
					String line = getContent(index++);
					if (line != null && !line.equals("")) {
						int epoch = Integer.parseInt(jedis.get("curEpochNumber"));
						if (curEpochNumber != epoch) {
							triggerChange(epoch);
						}
						boolean isFirst = (Math.random() > 0.5 ? true : false);
						emitData(line, isFirst);
					}
				}
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector=collector;
		jedis=getConnectedJedis();
		curEpochNumber=0;
		joinerTaskPhysicalIDs =context.getComponentTasks(joinerID);
		this.rowNodes=new HashMap<>();
		List<AssignNode> rowItems=new ArrayList<AssignNode>();
		AssignNode node=new AssignNode();
		node.index=0;
		node.isSpecial=false;
		rowItems.add(node);
		this.rowNodes.put(0, rowItems);
		this.colNodes=new HashMap<>();
		this.colNodes.put(0, rowItems);
		this.taskId=context.getThisTaskIndex();
		this.ticktime=0;
		this.totalData=jedis.llen(dataSource);
		
	}

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		List<String> dataFields=new ArrayList<String>();
		dataFields.add(TupleProperties.COPM_INDEX);
		dataFields.add(TupleProperties.TUPLE);
		dataFields.add(TupleProperties.EPOCH);
		dataFields.add(TupleProperties.SPECIAL);
		//数据流
		declarer.declareStream(Systemparameters.DATA_STREAM,new Fields(dataFields));
		//信号流
		declarer.declareStream(Systemparameters.ThetaControllerSignal, new Fields(TupleProperties.MESSAGE,TupleProperties.TICKTIME));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private Jedis getConnectedJedis() {
		if(jedis==null)
			jedis=new Jedis(host,port);
		return jedis;
	}
	
	private void emitData(String record,boolean isFirst){
		String[] items=record.split("\\|");
		List<String> tuple=new ArrayList<String>();
		tuple.add(index+"");
		for(int i=0;i<items.length;i++){
			tuple.add(items[i]);
		}
		tuple.add(0, index+"");
		index++;
		int emitterIndex=(isFirst==true?1:2);
		
		List<AssignNode> targetIndexes=this.getAssignedJoiners(isFirst);
		for(int i=0;i<targetIndexes.size();i++){
			/* 数据流tuple的各个字段：数据源编号
			 *                     实际数据(list）
			 *                     当前的epoch编号
			 *                     是否是发往最后一列或者一行的特殊节点
			 */
			Values value=new Values(emitterIndex,tuple,curEpochNumber,targetIndexes.get(i).isSpecial);
			_collector.emitDirect(joinerTaskPhysicalIDs.get(targetIndexes.get(i).index),Systemparameters.DATA_STREAM,value,record);
		}
	}
	

	// 更新新的矩阵
	private void triggerChange(int epoch){
		curEpochNumber = epoch;
		String newSchemeInfo = jedis.get("newSchemeInfo");
		curMatrix = stringToMatrix(newSchemeInfo);
		String matrix = jedis.get("newMap");
		getMapping(matrix);
	}
	
	public List<AssignNode> getAssignedJoiners(boolean isFirst){
		Random rand=new Random();
		if(isFirst){
			int rowIndex=rand.nextInt(this.rowNodes.size());
			return this.rowNodes.get(rowIndex);
		}else{
			int colIndex=rand.nextInt(this.colNodes.size());
			return this.colNodes.get(colIndex);
		}
	}
	
	private void initialize(){
		this.rowNodes=new HashMap<>();
		this.colNodes=new HashMap<>();
	}
	
	private void getMapping(String newMap){
		map = new int[curMatrix.row][curMatrix.col];
		String[] info = newMap.split("\\|");
		for(int i = 0; i < info.length; i++){
			String[] cell = info[i].split(" ");
			for(int j=0;j<cell.length;j++){
				map[i][j]=Integer.parseInt(cell[j]);
			}
		}
		getRowAndColNodes();
	}
	
	public void getRowAndColNodes(){
		this.initialize();
		int lastLength = 0;
		if(curMatrix.isUpRight){
			lastLength=curMatrix.row-curMatrix.discardCount;
			for(int j=0;j<curMatrix.col-1;j++){
				List<AssignNode> colItems=new ArrayList<AssignNode>();
				for(int i=0;i<curMatrix.row;i++){
					AssignNode node=new AssignNode();
					node.index=map[i][j];
					node.isSpecial=false;
					colItems.add(node);
				}
				this.colNodes.put(j, colItems);
			}
			List<AssignNode> colItems=new ArrayList<AssignNode>();
			for(int i=0;i<lastLength;i++){
				AssignNode node=new AssignNode();
				node.index=map[i][curMatrix.col-1];
				node.isSpecial=false;
				colItems.add(node);
			}
			this.colNodes.put(curMatrix.col-1, colItems);
			int k=0;
			for(int i=0;i<lastLength;i++){
				List<AssignNode> rowItems=new ArrayList<AssignNode>();
				for(int j=0;j<curMatrix.col;j++){
					AssignNode node=new AssignNode();
					node.index=map[i][j];
					node.isSpecial=false;
					rowItems.add(node);
				}
				for(int m=0;m<lastLength;m++){
					this.rowNodes.put(k++, rowItems);
				}
			}
			for(int i=lastLength;i<curMatrix.row;i++){
				List<AssignNode> rowItems=new ArrayList<AssignNode>();
				for(int j=0;j<curMatrix.col-1;j++){
					AssignNode node=new AssignNode();
					node.index=map[i][j];
					node.isSpecial=false;
					rowItems.add(node);
				}
				for(int m=0;m<lastLength;m++){
					List<AssignNode> rowItems2=new ArrayList<AssignNode>();
					rowItems2.addAll(rowItems);
					AssignNode node=new AssignNode();
					node.index=map[m][curMatrix.col-1];
					node.isSpecial=true;
					rowItems2.add(node);
					this.rowNodes.put(k++, rowItems2);
				}
			}
		}else{
			lastLength=curMatrix.col-curMatrix.discardCount;
			for(int i=0;i<curMatrix.row-1;i++){
				List<AssignNode> rowItems=new ArrayList<AssignNode>();
				for(int j=0;j<curMatrix.col;j++){
					AssignNode node=new AssignNode();
					node.index=map[i][j];
					node.isSpecial=false;
					rowItems.add(node);
				}
				this.rowNodes.put(i, rowItems);
			}
			List<AssignNode> rowItems=new ArrayList<AssignNode>();
			for(int j=0;j<lastLength;j++){
				AssignNode node=new AssignNode();
				node.index=map[curMatrix.row-1][j];
				node.isSpecial=false;
				rowItems.add(node);
			}
			this.rowNodes.put(curMatrix.row-1, rowItems);
			int k=0;
			for(int j=0;j<lastLength;j++){
				List<AssignNode> colItems=new ArrayList<AssignNode>();
				for(int i=0;i<curMatrix.row;i++){
					AssignNode node=new AssignNode();
					node.index=map[i][j];
					node.isSpecial=false;
					colItems.add(node);
				}
				for(int m=0;m<lastLength;m++){
					this.colNodes.put(k++, colItems);
				}
			}
			for(int j=lastLength;j<curMatrix.col;j++){
				List<AssignNode> colItems=new ArrayList<AssignNode>();
				for(int i=0;i<curMatrix.row-1;i++){
					AssignNode node=new AssignNode();
					node.index=map[i][j];
					node.isSpecial=false;
					colItems.add(node);
				}
				for(int m=0;m<lastLength;m++){
					List<AssignNode> colItems2=new ArrayList<AssignNode>();
					colItems2.addAll(colItems);
					AssignNode node=new AssignNode();
					node.index=map[curMatrix.row-1][m];
					node.isSpecial=true;
					colItems2.add(node);
					this.colNodes.put(k++, colItems2);
				}
			}
		}
	}
	
	private Matrix stringToMatrix(String str){
		if(str==null){
			throw new RuntimeException("Redis中没有新的matrix!");
		}
		String[] strs=str.split(" ");
		int[] attr=new int[strs.length];
		for(int i=0;i<strs.length;i++)
			attr[i]=Integer.parseInt(strs[i]);
		return new Matrix(attr);
	}
	
	public void fun(int[][] map,Matrix matrix){
		this.initialize();
		this.map=map;
		this.curMatrix=matrix;
	}
	
	public static void main(String[] args){
		int[][] map={{0,1,2,3,4,5},{6,7,8,9,10,11},{12,13,14,15,16,17},{18,19,-1,-1,-1,-1}};
		Matrix matrix=new Matrix(4,6,20,4,0);
		SpoutMFM sm=new SpoutMFM();
		sm.fun(map, matrix);
		long begin=System.currentTimeMillis();
		sm.getRowAndColNodes();
		System.out.println(System.currentTimeMillis()-begin);
		long begin2=System.currentTimeMillis();
		sm.getAssignedJoiners(true);
		System.out.println(System.currentTimeMillis()-begin2);
	}

}

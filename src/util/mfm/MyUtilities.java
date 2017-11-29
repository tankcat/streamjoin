package util.mfm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;


public class MyUtilities {
	
	
	public static final String RESHUFFLER_BOLT="reshuffler-bolt";
	public static final String JOINER_BOLT="joiner-bolt";
	public static final String TEST_TOPOLOGY="testTopology";
	public static final String ADVANCED_TOPOLOGY="advancedTopology";
	public static FileWriter fw=null;
	
	public static final String CONTROLLER_BOLT="controller-bolt";
	public static final String SLIDEWINDOWJOINER_BOLT="slide-window-joiner-bolt";
	public static final String CUSTOMER_SPOUT="customer-spout";
	public static final String CUSTOMER="customer";
	public static final String ORDER_SPOUT="order-spout";
	public static final String ORDER="order";
	
	
	public static void write(String fileName,String content){
	
	
		if(fw==null){
			try {
				fw=new FileWriter(fileName, true);
				fw.write(content+"\n");
				fw.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}else{
			try {
				fw.write(content+"\n");
				fw.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	
	public static List<ArrayList<String>> fileToMap(String filePath){
		final List<ArrayList<String>> fileItems=new ArrayList<ArrayList<String>>();
		String line;
		try {
			final BufferedReader reader = new BufferedReader(new FileReader(
					new File(filePath)));
			while((line=reader.readLine())!=null){
				line=line.trim();
				if(line.length()!=0 && line.charAt(0)!='\n' &&line.charAt(0)!='\r'){
					final String args[] =line.split("\\|");
					fileItems.add(new ArrayList<String>(Arrays.asList(args)));
				}
			}
		} catch (final Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileItems;
	}
	
	/**
	 * ���ж�ȡ����Դ�ļ������ַ�������ʽ�洢ÿ�����ݼ�¼
	 * @param filePath ���� �ļ�·��
	 * @return List<String> ���� ���ݼ�¼����
	 */
	public static List<String> fileToList(String filePath){
		final List<String> fileItems=new ArrayList<String>();
		String line;
		try {
			final BufferedReader reader=new BufferedReader(new FileReader(new File(filePath)));
			while((line=reader.readLine())!=null){
				line=line.trim();
				if(line.length()!=0&&line.charAt(0)!='\n'&&line.charAt(0)!='\r'){
					fileItems.add(line);
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileItems;
	}
	
	/**
	 * ����Reshuffler taskID���ھ�����Ѱ��ͬһ��Ԫ�ڵ�Joiner 
	 * @param matrix ���� Join Matrix
	 * @param number ���� Reshuffler Task Physical ID
	 * @return
	 */
	
	

	
	
	/**
	 * ��������������������ַ�����ʽ���з�
	 * @param dim ���� ���������������
	 * @return
	 */
	public static int[] getDimension(String dim){
		final String[] dimension=dim.split("-");
		return new int[]{Integer.parseInt(new String(dimension[0])),Integer.parseInt(new String(dimension[1]))};
	}

	public static int getParentTasksNum(TopologyContext tc, StormEmitter emitter1,
			StormEmitter... emittersArray){
		// TODO Auto-generated method stub
		final List<StormEmitter> emittersList=new ArrayList<StormEmitter>();
		emittersList.add(emitter1);
		emittersList.addAll(Arrays.asList(emittersArray));
		return getParentTasksNum(tc,emittersList);
	}
	
	/**
	 * ���Parent��Joiner����task����
	 * @param tc
	 * @param emittersList
	 * @return
	 */
	public static int getParentTasksNum(TopologyContext tc,List<StormEmitter> emittersList){
		int result=0;
		for(final StormEmitter emitter:emittersList){
			final String[] ids=emitter.getEmitterIDs();
			for(final String id:ids){
				result+=tc.getComponentTasks(id).size();
			}
		}
		return result;
	}
	
	/**
	 * ��������ϰ���|ƴ�ӳ�һ�����ݼ�¼
	 * @param tupleList ���� �������
	 * @return String ���� һ�����ݼ�¼
	 */
	public static String tupleListToString(List<String> tupleList){
		String tupleString="";
		for(int i=0;i<tupleList.size();i++){
			if(i==tupleList.size()-1)
				tupleString+=tupleList.get(i);
			else
				tupleString+=tupleList.get(i)+"|";
		}
		return tupleString;
	}

	/**
	 * �����ݼ�¼����|�зֳ�columnValue
	 * @param tupleString ���� һ�����ݼ�¼
	 * @return List<String> ���� ��¼���������
	 */
	public static List<String> stringToTuple(String tupleString) {
		// TODO Auto-generated method stub
		final String[] columnValues=tupleString.split("\\|");
		return new ArrayList<String>(Arrays.asList(columnValues));
	}
	
	public static void fun(String fileName1,String fileName2){
		Map<String,ArrayList<String>> customers=filetomap(fileName1);
		Map<String,ArrayList<String>> orders=filetomap(fileName2);
		Map<String,Integer> count=new HashMap<String,Integer>();
		Iterator iteratorCustomer=customers.entrySet().iterator();
		Iterator iteratorOrder=orders.entrySet().iterator();
		while(iteratorOrder.hasNext()){
			Map.Entry<String, ArrayList<String>> entry=(Entry<String, ArrayList<String>>) iteratorOrder.next();
			String orderKey=entry.getKey();
			ArrayList<String> orderItem=entry.getValue();
			String customerKey=orderItem.get(1);
			if(customers.containsKey(customerKey)){
				if(count.containsKey(customerKey)){
					count.put(customerKey, count.get(customerKey)+1);
				}else{
					count.put(customerKey, 1);
				}
				System.out.println(customerKey);
			}
		}
		int num=0;
		Iterator iteratorCount=count.entrySet().iterator();
		while(iteratorCount.hasNext()){
			Map.Entry<String, Integer> entry=(Entry<String, Integer>) iteratorCount.next();
			System.out.println(entry.getKey()+"="+entry.getValue());
			num+=entry.getValue();
		}
		System.out.println(num);
	}
	
	public static Map<String,ArrayList<String>> filetomap(String filename){
		Map<String,ArrayList<String>> fileItems=new HashMap<String,ArrayList<String>>();
		String line;
		try {
			final BufferedReader reader = new BufferedReader(new FileReader(
					new File(filename)));
			while((line=reader.readLine())!=null){
				line=line.trim();
				if(line.length()!=0 && line.charAt(0)!='\n' &&line.charAt(0)!='\r'){
					final String args[] =line.split("\\|");
					fileItems.put(args[0], new ArrayList<String>(Arrays.asList(args)));
				}
			}
		} catch (final Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileItems;
	}
	
	public static void main(String[] args){
		MyUtilities.fun("TestSource/customer2.tbl", "TestSource/orders2.tbl");
		
	}
	
	public static void processFinalAck(OutputCollector collector){
		Values values=new Values();
		values.add("N/A");
		//List<String> lastTuple=new ArrayList<String>(Arrays.asList(Systemparameters.LAST_ACK));
		values.add(new ArrayList<String>(
				Arrays.asList(Systemparameters.LAST_ACK)));
		collector.emit(values);
	}
	
	public static boolean isFinalAck(String tupleString){
		return tupleString.equals(Systemparameters.LAST_ACK);
	}
	
	public static String toMemoryInfo() {
		 
	       Runtime currRuntime = Runtime.getRuntime ();
	       int nFreeMemory = ( int ) (currRuntime.freeMemory() / 1024 / 1024);
	       int nTotalMemory = ( int ) (currRuntime.totalMemory() / 1024 / 1024);
	       return nFreeMemory + "M/" + nTotalMemory + "M(free/total)" ;
	}
}

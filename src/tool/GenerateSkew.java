package tool;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.math3.distribution.ZipfDistribution;

public class GenerateSkew {
	public static int uniqueKeyNum=150;
	public static int tupleNum=480000;
	public static double skew=0.5;
	public static ArrayList<Integer> beishuList=new ArrayList<Integer>();
	public static ArrayList<Double> uniqueKeyList=new ArrayList<Double>();
	public static LinkedList<Integer> keyList=new LinkedList<>();
	
	public static void writeToDisk(String fileName) throws IOException{
		getKeys();
		//Collections.shuffle(keyList);
		BufferedWriter writer = FileProcess.getWriter(fileName);
		String info="";
		for (Integer i:keyList) {
			FileProcess.write(i+"",writer);
		}

	}

	public static void getKeys(){
		for(int i=0;i<uniqueKeyNum;i++){
			int account=beishuList.get(i);
			for(int j=0;j<account;j++){
				keyList.add(i);
			}
		}
		Collections.shuffle(keyList);
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		ZipfDistribution dist=new ZipfDistribution(uniqueKeyNum,skew);
		for(int i=1;i<=uniqueKeyNum;i++){
			uniqueKeyList.add(dist.probability(i));
			System.out.println(dist.probability(i));
		}
		Collections.shuffle(uniqueKeyList);
		//int sum=0;
		for(int i=0;i<uniqueKeyList.size();i++){
			beishuList.add((int)((uniqueKeyList.get(i)*tupleNum)));
		}
		
		System.out.println("开始写入");
		String fileName="DataSource/keys_48w_150_0_5.txt";
		writeToDisk(fileName);
		System.out.println("结束写入");
	}

}

package tool;

import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class FileProcess implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6237017666448919161L;

	public static BufferedWriter getWriter(String fileName){
		BufferedWriter bw=null;
		try {
			bw=new BufferedWriter(new FileWriter(new File(fileName),false));
		} catch (IOException e) {
				// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return bw;
	}
	
	public static BufferedReader getReader(String fileName){
		BufferedReader br=null;
		try{
			br=new BufferedReader(new FileReader(new File(fileName)));
		}catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return br;
	}
	
	public static void write(String str,BufferedWriter bw){
		if(bw!=null){
			try {
				bw.write(str+'\n');
				bw.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static String  read(BufferedReader br){
		try {
			return br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	public static void close(BufferedWriter bw){
		if(bw!=null){
			try {
				bw.close();
				bw=null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public static void close(BufferedReader br){
		if(br!=null){
			try{
				br.close();
				br=null;
			}catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		}
	}
	

	
	public static void main(String[] args) throws IOException {
		//FileProcess.generateSkewData();
		String fileName="DataSource/orders.tbl";
		BufferedReader reader=FileProcess.getReader(fileName);
		Jedis jedis= new Jedis(SystemParameters.host,SystemParameters.port);
		String line=reader.readLine();
		System.out.println("开始读取并写入Redis:"+fileName);
		while(line != null && !line.equals("")){
			jedis.lpush("orders",line);
			line=reader.readLine();
		}
		FileProcess.close(reader);
		System.out.println("读写结束！");
	}
}

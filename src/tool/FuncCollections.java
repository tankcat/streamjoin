package tool;

import java.util.ArrayList;
import java.util.List;

public class FuncCollections {
	
	public static List<Integer> getKeys(int key,int abs){
		int begin=key-abs;
		int end=key+abs;
		if(begin<0)
			begin=0;
		List<Integer> keys=new ArrayList<Integer>();
		for(int i=begin;i<=end;i++){
			keys.add(i);
		}
		return keys;
	}
}

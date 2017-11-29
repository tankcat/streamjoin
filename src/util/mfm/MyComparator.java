package util.mfm;

import java.io.Serializable;
import java.util.Comparator;

public class MyComparator implements Comparator<Integer>,Serializable{


	@Override
	public int compare(Integer o1, Integer o2) {
		// TODO Auto-generated method stub
		return o1.compareTo(o2);
	}

}

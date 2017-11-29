package util;

import java.io.Serializable;
import java.util.Comparator;

import bolt.mfm.migrationplan.Begin_End;


public class MyCompare implements Comparator<Begin_End>,Serializable{

	@Override
	public int compare(Begin_End o1, Begin_End o2) {
		// TODO Auto-generated method stub
		if(o1.getBegin()<o2.getBegin())
			return 1;
		else if(o1.getBegin()==o2.getBegin())
			return 0;
		else return -1;
	}
	
}

package util.mfm;

import java.util.TreeMap;


public class Quadruple {
	/*public MyTupleStorage affectedStorage,oppositeStorage;
	public List<MyTupleStorage> oppositeStorages;
	public Quadruple(MyTupleStorage affectedStorage, MyTupleStorage oppositeStorage) {
		this.affectedStorage = affectedStorage;
		this.oppositeStorage = oppositeStorage;
	}*/
	public TreeMap<Integer,String> affected,opposite;
	public Quadruple(TreeMap<Integer,String> affected,TreeMap<Integer,String> opposite){
		this.affected=affected;
		this.opposite=opposite;
	}
	
}

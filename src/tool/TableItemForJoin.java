package tool;

import java.io.Serializable;

public class TableItemForJoin implements Serializable ,Comparable<TableItemForJoin>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8388515532056742332L;
	
	public int count;
	public int taskIndex;
	public TableItemForJoin(int count, int taskIndex) {
		super();
		this.count = count;
		this.taskIndex = taskIndex;
	}
	
	@Override
	public int compareTo(TableItemForJoin other) {
		// TODO Auto-generated method stub
		return this.count-other.count;
	}
	
	@Override
	public String toString(){
		String str =" [taskIndex = "+taskIndex+",count = "+count+"] ";
		return str;
	}
}

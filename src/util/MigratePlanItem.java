package util;

import java.io.Serializable;

public class MigratePlanItem implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3079399368063670338L;
	private int from;
	private int to;
	private int key;
	private int count;
	
	public MigratePlanItem(int from,int to,int key,int count){
		this.from=from;
		this.to=to;
		this.key=key;
		this.count=count;
	}
	
	@Override
	public String toString(){
		String str="from = "+from+" to = "+to+" , key = "+key+" , count = "+count;
		return str;
	}

	public int getFrom() {
		return from;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public int getTo() {
		return to;
	}

	public void setTo(int to) {
		this.to = to;
	}

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}

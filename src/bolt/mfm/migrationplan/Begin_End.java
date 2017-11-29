package bolt.mfm.migrationplan;

import java.io.Serializable;

public class Begin_End implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -5050821196706010409L;
	private double begin;
	private double end;
	private boolean unregular;
	public double getBegin() {
		return begin;
	}
	public void setBegin(double begin) {
		this.begin = begin;
	}
	public double getEnd() {
		return end;
	}
	public void setEnd(double end) {
		this.end = end;
	}
	public boolean isUnregular() {
		return unregular;
	}
	public void setUnregular(boolean unregular) {
		this.unregular = unregular;
	}
}

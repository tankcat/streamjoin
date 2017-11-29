package util.mfm;

import java.io.Serializable;

public class MigVolume implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -280208604888553058L;
	public int migIndex;
	public int taskIndex;
	public long migR;
	public long migS;
	public MigVolume(int migIndex,int taskIndex,long migR,long migS){
		this.migIndex=migIndex;
		this.taskIndex=taskIndex;
		this.migR=migR;
		this.migS=migS;
	}
}

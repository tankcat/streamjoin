package util.mfm;

import java.io.Serializable;

public class ExperimentInfo implements Serializable {
	private static final long serialVersionUID = 8533679571613181752L;
	public long generate_plan_time;
	public double totalR,totalS;
	public int node_number;
	public ExperimentInfo(long gap,double R,double S,int node){
		this.generate_plan_time=gap;
		this.totalR=R;
		this.totalS=S;
		this.node_number=node;
	}
}

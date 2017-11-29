package bolt.mfm.migrationplan;

import java.util.Vector;


public class new_Node_optimal {
	private boolean activity;
	
	private Vector<Begin_End> r_list;
	private Vector<Begin_End> s_list;
	private int position_in_list;
	
	
	public boolean isActivity() {
		return activity;
	}
	public void setActivity(boolean activity) {
		this.activity = activity;
	}
	public int getPosition_in_list() {
		return position_in_list;
	}
	public void setPosition_in_list(int position_in_list) {
		this.position_in_list = position_in_list;
	}
	public Vector<Begin_End> getR_list() {
		return r_list;
	}
	public void setR_list(Vector<Begin_End> r_list) {
		this.r_list = r_list;
	}
	public Vector<Begin_End> getS_list() {
		return s_list;
	}
	public void setS_list(Vector<Begin_End> s_list) {
		this.s_list = s_list;
	}
}

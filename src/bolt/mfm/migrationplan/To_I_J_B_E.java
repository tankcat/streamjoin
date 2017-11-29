package bolt.mfm.migrationplan;

import java.io.Serializable;
import java.util.Vector;

public class To_I_J_B_E implements Serializable{

	private int i_old;
	private int j_old;
	private int i_new;
	private int j_new;


	
	private Vector<Begin_End> be_send_list;
	
	public int getI_old() {
		return i_old;
	}
	public void setI_old(int i_old) {
		this.i_old = i_old;
	}
	public int getJ_old() {
		return j_old;
	}
	public void setJ_old(int j_old) {
		this.j_old = j_old;
	}
	public int getI_new() {
		return i_new;
	}
	public void setI_new(int i_new) {
		this.i_new = i_new;
	}
	public int getJ_new() {
		return j_new;
	}
	public void setJ_new(int j_new) {
		this.j_new = j_new;
	}
	public Vector<Begin_End> getBe_send_list() {
		return be_send_list;
	}
	public void setBe_send_list(Vector<Begin_End> be_send_list) {
		this.be_send_list = be_send_list;
	}
}

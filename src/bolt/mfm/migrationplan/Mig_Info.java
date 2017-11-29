package bolt.mfm.migrationplan;

import java.util.Vector;

public class Mig_Info {
	private int i_old;
	private int j_old;
	
	private int i_new_from;
	private int j_new_from;
	
	private Vector<Begin_End> r_discard_list;
	private Vector<Begin_End> s_discard_list;
	
	private Vector<To_I_J_B_E> r_i_j_list;
	private Vector<To_I_J_B_E> s_i_j_list;
	
	public Vector<To_I_J_B_E> getR_i_j_list() {
		return r_i_j_list;
	}
	public void setR_i_j_list(Vector<To_I_J_B_E> r_i_j_list) {
		this.r_i_j_list = r_i_j_list;
	}
	public Vector<Begin_End> getR_discard_list() {
		return r_discard_list;
	}
	public void setR_discard_list(Vector<Begin_End> r_discard_list) {
		this.r_discard_list = r_discard_list;
	}
	public Vector<To_I_J_B_E> getS_i_j_list() {
		return s_i_j_list;
	}
	public void setS_i_j_list(Vector<To_I_J_B_E> s_i_j_list) {
		this.s_i_j_list = s_i_j_list;
	}
	public Vector<Begin_End> getS_discard_list() {
		return s_discard_list;
	}
	public void setS_discard_list(Vector<Begin_End> s_discard_list) {
		this.s_discard_list = s_discard_list;
	}
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
	public int getI_new_from() {
		return i_new_from;
	}
	public void setI_new_from(int i_new_from) {
		this.i_new_from = i_new_from;
	}
	public int getJ_new_from() {
		return j_new_from;
	}
	public void setJ_new_from(int j_new_from) {
		this.j_new_from = j_new_from;
	}
	}

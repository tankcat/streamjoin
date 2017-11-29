package bolt.mfm.migrationplan;

import java.util.Vector;

public class Old_New_Relation_optimal {
	private int i_old;
	private int j_old;
	private int i_new;
	private int j_new;
	
	private double r_re;
	private double S_re;
	private double all_re;
	
	private Begin_End r_had_old;
	private Begin_End s_had_old;
	
	private Begin_End r_had_new;
	private Begin_End s_had_new;
	
	private Vector<Begin_End> r_should_new_list;
	private Vector<Begin_End> s_should_new_list;

	
	private Vector<Begin_End> r_need_list;
	private Vector<Begin_End> s_need_list;
	
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
	public double getR_re() {
		return r_re;
	}
	public void setR_re(double r_re) {
		this.r_re = r_re;
	}
	public double getAll_re() {
		return all_re;
	}
	public void setAll_re(double all_re) {
		this.all_re = all_re;
	}
	public double getS_re() {
		return S_re;
	}
	public void setS_re(double s_re) {
		S_re = s_re;
	}
	public Begin_End getR_had_new() {
		return r_had_new;
	}
	public void setR_had_new(Begin_End r_had_new) {
		this.r_had_new = r_had_new;
	}
	public Begin_End getS_had_new() {
		return s_had_new;
	}
	public void setS_had_new(Begin_End s_had_new) {
		this.s_had_new = s_had_new;
	}
	public Vector<Begin_End> getR_need_list() {
		return r_need_list;
	}
	public void setR_need_list(Vector<Begin_End> r_need_list) {
		this.r_need_list = r_need_list;
	}
	public Vector<Begin_End> getS_need_list() {
		return s_need_list;
	}
	public void setS_need_list(Vector<Begin_End> s_need_list) {
		this.s_need_list = s_need_list;
	}
	public Begin_End getR_had_old() {
		return r_had_old;
	}
	public void setR_had_old(Begin_End r_had_old) {
		this.r_had_old = r_had_old;
	}
	public Begin_End getS_had_old() {
		return s_had_old;
	}
	public void setS_had_old(Begin_End s_had_old) {
		this.s_had_old = s_had_old;
	}
	public Vector<Begin_End> getR_should_new_list() {
		return r_should_new_list;
	}
	public void setR_should_new_list(Vector<Begin_End> r_should_new_list) {
		this.r_should_new_list = r_should_new_list;
	}
	public Vector<Begin_End> getS_should_new_list() {
		return s_should_new_list;
	}
	public void setS_should_new_list(Vector<Begin_End> s_should_new_list) {
		this.s_should_new_list = s_should_new_list;
	}
}

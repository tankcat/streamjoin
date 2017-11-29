package util.mfm;

import java.io.Serializable;
import java.util.Vector;

import bolt.mfm.migrationplan.Begin_End;

public class SendInfo implements Serializable{
	private static final long serialVersionUID = -7977690860685519286L;
	public int targetIndex;
	public Vector<Begin_End> be_send_list;
}
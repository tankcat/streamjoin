package util.mfm;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Vector;

import bolt.mfm.migrationplan.Begin_End;
import org.apache.commons.math3.util.Pair;


public class MigrationInfo implements Serializable{
	private static final long serialVersionUID = 3130173558309039984L;
	public Vector<Begin_End> r_discard;
	public Vector<Begin_End> s_discard;
	public Vector<SendInfo> r_send;
	public Vector<SendInfo> s_send;
	public boolean isActive;
	public boolean isNewNode;
	public HashMap<Integer,Double[]> mig_info;

	public int selfindex;

	public void getMigrateVolume(){
		mig_info=new HashMap<>();
		if(r_send!=null&&r_send.size()>0) {
			for (SendInfo r_item : r_send) {
				if(r_item!=null) {
					int target = r_item.targetIndex;
					if(target==selfindex)
						continue;
					Vector<Begin_End> send_list_r = r_item.be_send_list;
					if(send_list_r!=null && send_list_r.size()>0) {
						double tmpR = 0;
						for (Begin_End be : send_list_r) {
							if(be!=null) {
								tmpR += (be.getEnd() - be.getBegin());
							}
						}
						if (!mig_info.containsKey(target)) {
							mig_info.put(target, new Double[2]);
						}
						mig_info.get(target)[0] = tmpR;
					}
				}
			}
		}
		if(s_send!=null&&s_send.size()>0) {
			for (SendInfo s_item : s_send) {
				if(s_item!=null) {
					int target = s_item.targetIndex;
					if(target==selfindex)
						continue;
					Vector<Begin_End> send_list_s = s_item.be_send_list;
					if(send_list_s!=null&&send_list_s.size()>0) {
						double tmpS = 0;
						for (Begin_End be : send_list_s) {
							if(be!=null) {
								tmpS += (be.getEnd() - be.getBegin());
							}
						}
						if (!mig_info.containsKey(target)) {
							mig_info.put(target, new Double[2]);
						}
						mig_info.get(target)[1] = tmpS;
					}
				}
			}
		}
	}
}

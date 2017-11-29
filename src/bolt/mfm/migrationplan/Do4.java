package bolt.mfm.migrationplan;

import java.util.Vector;

public class Do4 {

	public static void Merge_migration_plan(Vector<Mig_Info> migrationList) {
		// TODO Auto-generated method stub
		for(int i=0;i<migrationList.size();i++){
			int i_old1 = migrationList.get(i).getI_old();
			int j_old1 = migrationList.get(i).getJ_old();
			int i_new1 = migrationList.get(i).getI_new_from();
			int j_new1 = migrationList.get(i).getJ_new_from();
			for(int j=i;j<migrationList.size();j++){
				if(i==j){continue;}
				int i_old2 = migrationList.get(j).getI_old();
				int j_old2 = migrationList.get(j).getJ_old();
				int i_new2 = migrationList.get(j).getI_new_from();
				int j_new2 = migrationList.get(j).getJ_new_from();
				if(i_old1==i_old2 && j_old1==j_old2 && i_new1==i_new2 && j_new1==j_new2){//��jlist�����ж����ŵ�ilist
					for(int r_discard=0; r_discard<migrationList.get(j).getR_discard_list().size();r_discard++){//����discard  R
						double r_d_b = migrationList.get(j).getR_discard_list().get(r_discard).getBegin();
						double r_d_e = migrationList.get(j).getR_discard_list().get(r_discard).getEnd();
						Begin_End be = new Begin_End();
						be.setBegin(r_d_b);
						be.setEnd(r_d_e);
						migrationList.get(i).getR_discard_list().add(be);
					}
					for(int s_discard=0; s_discard<migrationList.get(j).getS_discard_list().size();s_discard++){//����discard  S
						double s_d_b = migrationList.get(j).getS_discard_list().get(s_discard).getBegin();
						double s_d_e = migrationList.get(j).getS_discard_list().get(s_discard).getEnd();
						Begin_End be = new Begin_End();
						be.setBegin(s_d_b);
						be.setEnd(s_d_e);
						migrationList.get(i).getS_discard_list().add(be);
					}
					for(int r_to =0; r_to <migrationList.get(j).getR_i_j_list().size();r_to++){//���� To_I_J_B_E R
						int i_old = migrationList.get(j).getR_i_j_list().get(r_to).getI_old();
						int j_old = migrationList.get(j).getR_i_j_list().get(r_to).getJ_old();
						int i_new = migrationList.get(j).getR_i_j_list().get(r_to).getI_new();
						int j_new = migrationList.get(j).getR_i_j_list().get(r_to).getJ_new();
						
						Vector<Begin_End> r_be_s_list = new Vector<Begin_End>();
						for(int rbsl=0;rbsl<migrationList.get(j).getR_i_j_list().get(r_to).getBe_send_list().size();rbsl++){
							double begin = migrationList.get(j).getR_i_j_list().get(r_to).getBe_send_list().get(rbsl).getBegin();
						    double end = migrationList.get(j).getR_i_j_list().get(r_to).getBe_send_list().get(rbsl).getEnd();
						    boolean unregular = migrationList.get(j).getR_i_j_list().get(r_to).getBe_send_list().get(rbsl).isUnregular();

						    Begin_End be = new Begin_End();
						    be.setBegin(begin);
						    be.setEnd(end);
						    be.setUnregular(unregular);
						    r_be_s_list.add(be);
						}

						To_I_J_B_E r_t_ij = new To_I_J_B_E();
						r_t_ij.setI_old(i_old);
						r_t_ij.setJ_old(j_old);
						r_t_ij.setI_new(i_new);
						r_t_ij.setJ_new(j_new);
						r_t_ij.setBe_send_list(r_be_s_list);
						
						migrationList.get(i).getR_i_j_list().add(r_t_ij);
					}
					for(int s_to =0; s_to <migrationList.get(j).getS_i_j_list().size();s_to++){//���� To_I_J_B_E S
						//���� To_I_J_B_E R
						int i_old = migrationList.get(j).getS_i_j_list().get(s_to).getI_old();
						int j_old = migrationList.get(j).getS_i_j_list().get(s_to).getJ_old();
						int i_new = migrationList.get(j).getS_i_j_list().get(s_to).getI_new();
						int j_new = migrationList.get(j).getS_i_j_list().get(s_to).getJ_new();
						
						Vector<Begin_End> s_be_s_list = new Vector<Begin_End>();
						for(int sbsl=0;sbsl<migrationList.get(j).getS_i_j_list().get(s_to).getBe_send_list().size();sbsl++){
							double begin = migrationList.get(j).getS_i_j_list().get(s_to).getBe_send_list().get(sbsl).getBegin();
						    double end = migrationList.get(j).getS_i_j_list().get(s_to).getBe_send_list().get(sbsl).getEnd();
						    boolean unregular = migrationList.get(j).getS_i_j_list().get(s_to).getBe_send_list().get(sbsl).isUnregular();

						    Begin_End be = new Begin_End();
						    be.setBegin(begin);
						    be.setEnd(end);
						    be.setUnregular(unregular);
						    s_be_s_list.add(be);
						}

						To_I_J_B_E s_t_ij = new To_I_J_B_E();
						s_t_ij.setI_old(i_old);
						s_t_ij.setJ_old(j_old);
						s_t_ij.setI_new(i_new);
						s_t_ij.setJ_new(j_new);
						s_t_ij.setBe_send_list(s_be_s_list);
						
						migrationList.get(i).getS_i_j_list().add(s_t_ij);
					
					}
					migrationList.remove(j);
				    j--;
				}
			}
		}
	}

}

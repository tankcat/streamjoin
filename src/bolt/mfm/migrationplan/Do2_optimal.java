package bolt.mfm.migrationplan;

import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;




public class Do2_optimal {

	public static void make_map(old_Node_optimal[][] oldnodes_optimal, new_Node_optimal[][] newnodes_optimal) {
		// TODO Auto-generated method stub

		for(int i=0;i<oldnodes_optimal.length;i++){//对于旧节点
			for(int j=0;j<oldnodes_optimal[i].length;j++){
				if(oldnodes_optimal[i][j].isActivity()){
				for(int k=0;k<newnodes_optimal.length;k++){//对于新节点
					for(int l=0;l<newnodes_optimal[k].length;l++){
						if(newnodes_optimal[k][l].isActivity()){
						Old_New_Relation_optimal onr = new Old_New_Relation_optimal();
						onr.setI_old(i);
						onr.setJ_old(j);
						onr.setI_new(k);
						onr.setJ_new(l);
						
						//处理R的已有  和需求
						double r_had_begin =oldnodes_optimal[i][j].getR_begin();
						double r_had_end =  oldnodes_optimal[i][j].getR_end();
						Vector<Begin_End> r_should_new_list_temp = new Vector<Begin_End>();
						for(int rlist=0;rlist<newnodes_optimal[k][l].getR_list().size();rlist++){
							double begin = newnodes_optimal[k][l].getR_list().get(rlist).getBegin();
							double end = newnodes_optimal[k][l].getR_list().get(rlist).getEnd();
							Begin_End r_be = new Begin_End();
							r_be.setBegin(begin);
							r_be.setEnd(end);
							r_should_new_list_temp.add(r_be);
						}
						//处理S的已有  和需求
						double s_had_begin =oldnodes_optimal[i][j].getS_begin();
						double s_had_end =  oldnodes_optimal[i][j].getS_end();
						Vector<Begin_End> s_should_new_list_temp = new Vector<Begin_End>();
						for(int slist=0;slist<newnodes_optimal[k][l].getS_list().size();slist++){
							double begin = newnodes_optimal[k][l].getS_list().get(slist).getBegin();
							double end = newnodes_optimal[k][l].getS_list().get(slist).getEnd();
							Begin_End s_be = new Begin_End();
							s_be.setBegin(begin);
							s_be.setEnd(end);
							s_should_new_list_temp.add(s_be);
						}
						
						//将R的had封装
						Begin_End be_r_had = new Begin_End();
						be_r_had.setBegin(r_had_begin);
						be_r_had.setEnd(r_had_end);
						//将S的had封装
						Begin_End be_s_had = new Begin_End();
						be_s_had.setBegin(s_had_begin);
						be_s_had.setEnd(s_had_end);

						
						onr.setR_had_old(be_r_had);
						onr.setR_had_new(be_r_had);
						onr.setR_should_new_list(r_should_new_list_temp);
						
						onr.setS_had_old(be_s_had);
						onr.setS_had_new(be_s_had);
						onr.setS_should_new_list(s_should_new_list_temp);
						//**************计算R
						for(int r_should_num=0;r_should_num<r_should_new_list_temp.size();r_should_num++){
							double r_should_begin = r_should_new_list_temp.get(r_should_num).getBegin();
							double r_should_end =  r_should_new_list_temp.get(r_should_num).getEnd();
							double had_r = onr.getR_re();
						if(r_had_end<=r_should_begin||r_had_begin>=r_should_end){//1.不相交
							onr.setR_re(Tools_optimal.get_double(had_r+0));
						}else if(r_should_begin >= r_had_begin && r_should_end <= r_had_end){//关系2: had 全包含 should--》丢弃两端不需要的
							onr.setR_re(Tools_optimal.get_double(had_r + (r_should_end-r_should_begin)*Para.newR));
						}else if(r_should_begin <= r_had_begin && r_should_end >= r_had_end){//关系3: should 全包含  had--》全不丢弃
							onr.setR_re(Tools_optimal.get_double(had_r + (r_had_end-r_had_begin)*Para.newR));
						}else
						{//关系4: 部分的包含--》丢弃一端
							if(r_should_begin >= r_had_begin && r_should_end >= r_had_end){
								onr.setR_re(Tools_optimal.get_double(had_r + (r_had_end-r_should_begin)*Para.newR));
							}else{
								onr.setR_re(Tools_optimal.get_double(had_r + (r_should_end-r_had_begin)*Para.newR));
							}}}
						//**************计算S
						for(int s_should_num=0;s_should_num<s_should_new_list_temp.size();s_should_num++){
							double s_should_begin = s_should_new_list_temp.get(s_should_num).getBegin();
							double s_should_end =  s_should_new_list_temp.get(s_should_num).getEnd();
							double had_s = onr.getS_re();
						if(s_had_end<=s_should_begin||s_had_begin>=s_should_end){//1.不相交
							onr.setS_re(Tools_optimal.get_double(had_s+0));
						}else if(s_should_begin >= s_had_begin && s_should_end <= s_had_end){//关系2: had 全包含 should--》丢弃两端不需要的
							onr.setS_re(Tools_optimal.get_double(had_s+(s_should_end-s_should_begin)*Para.newS));
						}else if(s_should_begin <= s_had_begin && s_should_end >= s_had_end){//关系3: should 全包含  had--》全不丢弃
							onr.setS_re(Tools_optimal.get_double(had_s+(s_had_end-s_had_begin)*Para.newS));
						}else
						{//关系4: 部分的包含--》丢弃一端
							if(s_should_begin >= s_had_begin && s_should_end >= s_had_end){
								onr.setR_re(Tools_optimal.get_double(had_s+(s_had_end-s_should_begin)*Para.newS));
							}else{
								onr.setS_re(Tools_optimal.get_double(had_s+(s_should_end-s_had_begin)*Para.newS));
							}
					}}			
						onr.setAll_re(Tools_optimal.get_double(onr.getR_re()+onr.getS_re()));
						Para.relationList_optimal.add(onr);
						}
					}
				}
			}
		}
	}
		Collections.sort(Para.relationList_optimal, new Comparator<Old_New_Relation_optimal>() {
			@Override
			public int compare(Old_New_Relation_optimal o1, Old_New_Relation_optimal o2) {
				return -((Double)o1.getAll_re()).compareTo(o2.getAll_re());
			}
		});

		for(int i=0;i<Para.relationList_optimal.size();i++){
			/*System.out.println(i+" : "+Para.relationList_optimal.get(i).getI_old()+" "+
					Para.relationList_optimal.get(i).getJ_old()+" "+
					Para.relationList_optimal.get(i).getI_new()+" "+
					Para.relationList_optimal.get(i).getJ_new()+" "+
					Para.relationList_optimal.get(i).getR_re()+" "+
					Para.relationList_optimal.get(i).getS_re()+" "+
					Para.relationList_optimal.get(i).getAll_re());*/
		}
		clean_relation_list(Para.relationList_optimal);
		add_no_relation(Para.relationList_optimal,oldnodes_optimal,newnodes_optimal);
		
		//System.out.println("println the after clean: ");
		for(int i=0;i<Para.relationList_optimal.size();i++){
			/*System.out.println(i+" : "+
		            Para.relationList_optimal.get(i).getI_old()+" "+
					Para.relationList_optimal.get(i).getJ_old()+" "+
					Para.relationList_optimal.get(i).getI_new()+" "+
					Para.relationList_optimal.get(i).getJ_new()+" "+
					Para.relationList_optimal.get(i).getR_re()+" "+
					Para.relationList_optimal.get(i).getS_re()+" "+
					Para.relationList_optimal.get(i).getAll_re());*/
		}
	}

	private static void add_no_relation(Vector<Old_New_Relation_optimal> relationList, old_Node_optimal[][] oldnodes_optimal, new_Node_optimal[][] newnodes_optimal) {
		// TODO Auto-generated method stub
		boolean mark;
		//**************对于旧节点 多的情况
		for(int i=0;i<oldnodes_optimal.length;i++){
			for(int j=0;j<oldnodes_optimal[i].length;j++){
				if(oldnodes_optimal[i][j].isActivity()){
					mark = true;
					for(int k=0;k<relationList.size();k++){
						int old_i_in_list = relationList.get(k).getI_old();
						int old_j_in_list = relationList.get(k).getJ_old();
						if(i==old_i_in_list && j==old_j_in_list){
							mark = false;
							break;
						}
					}
					if(mark){
						Old_New_Relation_optimal onr = new Old_New_Relation_optimal();
						onr.setI_old(i);
						onr.setJ_old(j);
						onr.setI_new(65535);
						onr.setJ_new(65535);
						//对于旧节点只需要告诉节点有什么即可
						Begin_End r_be = new Begin_End();
						r_be.setBegin(oldnodes_optimal[i][j].getR_begin());
						r_be.setEnd(oldnodes_optimal[i][j].getR_end());
						
						Begin_End s_be = new Begin_End();
						s_be.setBegin(oldnodes_optimal[i][j].getS_begin());
						s_be.setEnd(oldnodes_optimal[i][j].getS_end());
						onr.setR_had_new(r_be);
						onr.setS_had_new(s_be);

						//装入应该有
						Vector<Begin_End> r_should_new_list_temp = new Vector<Begin_End>();
						
						Vector<Begin_End> s_should_new_list_temp = new Vector<Begin_End>();

						
						onr.setR_should_new_list(r_should_new_list_temp);
						onr.setS_should_new_list(s_should_new_list_temp);;	
						
						Para.relationList_optimal.add(onr);
					}
				}
			}
		}
		//**************对于新节点多的情况
		for(int i=0;i<newnodes_optimal.length;i++){//对于旧节点 多
			for(int j=0;j<newnodes_optimal[i].length;j++){
				if(newnodes_optimal[i][j].isActivity()){
					mark = true;
					for(int k=0;k<relationList.size();k++){
						int new_i_in_list = relationList.get(k).getI_new();
						int new_j_in_list = relationList.get(k).getJ_new();
						if(i==new_i_in_list && j==new_j_in_list){
							mark = false;
							break;
						}
					}
					if(mark){
						Old_New_Relation_optimal onr = new Old_New_Relation_optimal();
						onr.setI_old(65535);
						onr.setJ_old(65535);
						onr.setI_new(i);
						onr.setJ_new(j);
						//装入已有
						Begin_End r_be = new Begin_End();
						r_be.setBegin(0);
						r_be.setEnd(0);
						
						Begin_End s_be = new Begin_End();
						s_be.setBegin(0);
						s_be.setEnd(0);
						
						onr.setR_had_new(r_be);
						onr.setS_had_new(s_be);
						
						//装入应该有
						Vector<Begin_End> r_should_new_list_temp = new Vector<Begin_End>();
						for(int rlist=0;rlist<newnodes_optimal[i][j].getR_list().size();rlist++){
							double begin = newnodes_optimal[i][j].getR_list().get(rlist).getBegin();
							double end = newnodes_optimal[i][j].getR_list().get(rlist).getEnd();
							Begin_End r_be1 = new Begin_End();
							r_be1.setBegin(begin);
							r_be1.setEnd(end);
							r_should_new_list_temp.add(r_be1);
						}
						
						Vector<Begin_End> s_should_new_list_temp = new Vector<Begin_End>();
						for(int slist=0;slist<newnodes_optimal[i][j].getS_list().size();slist++){
							double begin = newnodes_optimal[i][j].getS_list().get(slist).getBegin();
							double end = newnodes_optimal[i][j].getS_list().get(slist).getEnd();
							Begin_End s_be1 = new Begin_End();
							s_be1.setBegin(begin);
							s_be1.setEnd(end);
							s_should_new_list_temp.add(s_be1);
						}

						onr.setR_should_new_list(r_should_new_list_temp);
						onr.setS_should_new_list(s_should_new_list_temp);;	
						
						Para.relationList_optimal.add(onr);
					}
				}
			}
		}
	}

	private static void clean_relation_list(
			Vector<Old_New_Relation_optimal> relationList) {
		// TODO Auto-generated method stub
		for(int i=0;i<relationList.size();i++){
			for(int j=i+1;j<relationList.size();j++){
				if(((relationList.get(i).getI_old()==relationList.get(j).getI_old())
						&&(relationList.get(i).getJ_old()==relationList.get(j).getJ_old()))
						||
						((relationList.get(i).getI_new()==relationList.get(j).getI_new())
								&&(relationList.get(i).getJ_new()==relationList.get(j).getJ_new()))){
					relationList.remove(j);
					j--;
				}
			}
		}
	}

}
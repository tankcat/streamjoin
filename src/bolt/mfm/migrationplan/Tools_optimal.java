package bolt.mfm.migrationplan;

import java.io.BufferedWriter;
import java.math.BigDecimal;
import java.util.Vector;

import tool.FileProcess;



public class Tools_optimal {
	public static void printnodes(new_Node_optimal[][] newnodes_optimal) {
		// TODO Auto-generated method stub
		for(int i=0;i<newnodes_optimal.length;i++){
			for(int j=0;j<newnodes_optimal[0].length;j++){
				if(newnodes_optimal[i][j].isActivity()){
					System.out.print("R:[");
					for(int shoud=0;shoud<newnodes_optimal[i][j].getR_list().size();shoud++){
					System.out.print(newnodes_optimal[i][j].getR_list().get(shoud).getBegin()+"-"+
					newnodes_optimal[i][j].getR_list().get(shoud).getEnd()+", ");
			}
					System.out.print("]");
				
				System.out.print("S:[");
				for(int shoud=0;shoud<newnodes_optimal[i][j].getS_list().size();shoud++){
				System.out.print(newnodes_optimal[i][j].getS_list().get(shoud).getBegin()+"-"+
				newnodes_optimal[i][j].getS_list().get(shoud).getEnd()+", ");
		}
				System.out.print("]\t\t");
			}
		}
			System.out.println();
			}
	}

	public static void printnodes(old_Node_optimal[][] oldnodes_optimal) {
		// TODO Auto-generated method stub
		for(int i=0;i<oldnodes_optimal.length;i++){
			for(int j=0;j<oldnodes_optimal[i].length;j++){
				if(oldnodes_optimal[i][j].isActivity()) {
					System.out.print(oldnodes_optimal[i][j].getR_begin()+"-"+oldnodes_optimal[i][j].getR_end()+"/"+oldnodes_optimal[i][j].getS_begin()+"-"+oldnodes_optimal[i][j].getS_end()+"\t\t");
				}
			}
			System.out.println();
		}
	}

	public static double get_double(double d) {
		// TODO Auto-generated method stub
		BigDecimal   b   =   new   BigDecimal(d);  
		return b.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
	}

	public static void print_need_to_file(Vector<Old_New_Relation_optimal> relationList_optimal,int changeIndex){
		BufferedWriter bw=FileProcess.getWriter("Experiment/scheme.txt");
		FileProcess.write("第"+changeIndex+"次制定迁移计划.",bw);
		FileProcess.write("====================此处打印需求信息===========================", bw);
		for(int i=0;i<relationList_optimal.size();i++){
			int i_old1=relationList_optimal.get(i).getI_old();
			int j_old1=relationList_optimal.get(i).getJ_old();
			int i_new1=relationList_optimal.get(i).getI_new();
			int j_new1=relationList_optimal.get(i).getJ_new();
			for(int j=0;j<relationList_optimal.get(i).getR_need_list().size();j++){
				FileProcess.write("旧节点： ["+i_old1+","+j_old1+"] 即是新节点：["+i_new1+","+j_new1+"] 还需要Ｒ　"+
						relationList_optimal.get(i).getR_need_list().get(j).getBegin()+" -> "+
						relationList_optimal.get(i).getR_need_list().get(j).getEnd(), bw);
			}
			for(int k=0;k<Para.relationList_optimal.get(i).getS_need_list().size();k++){
				FileProcess.write("旧节点： ["+i_old1+","+j_old1+"] 即是新节点：["+i_new1+","+j_new1+"] 还需要S "+
						relationList_optimal.get(i).getS_need_list().get(k).getBegin()+" -> "+
						relationList_optimal.get(i).getS_need_list().get(k).getEnd()
						,bw);
			}
		}
		FileProcess.close(bw);
	}
	
	
	
	public static void print_need(Vector<Old_New_Relation_optimal> relationList_optimal) {
		// TODO Auto-generated method stub
		//System.out.println("====================此处打印需求信息===========================");
		for(int i=0;i<relationList_optimal.size();i++){
			int i_old1=relationList_optimal.get(i).getI_old();
			int j_old1=relationList_optimal.get(i).getJ_old();
			int i_new1=relationList_optimal.get(i).getI_new();
			int j_new1=relationList_optimal.get(i).getJ_new();
			
			for(int j=0;j<relationList_optimal.get(i).getR_need_list().size();j++){
				/*System.out.println("旧节点： ["+i_old1+","+j_old1+"] 即是新节点：["+i_new1+","+j_new1+"] 还需要Ｒ　"+
						relationList_optimal.get(i).getR_need_list().get(j).getBegin()+" -> "+
						relationList_optimal.get(i).getR_need_list().get(j).getEnd()
						);*/
			}
			for(int k=0;k<Para.relationList_optimal.get(i).getS_need_list().size();k++){
				/*System.out.println("旧节点： ["+i_old1+","+j_old1+"] 即是新节点：["+i_new1+","+j_new1+"] 还需要S "+
						relationList_optimal.get(i).getS_need_list().get(k).getBegin()+" -> "+
						relationList_optimal.get(i).getS_need_list().get(k).getEnd()
						);*/
			}
		}
	}

	public static void print_discard_to_file(Vector<Mig_Info> migrationList){
		BufferedWriter bw=FileProcess.getWriter("Experiment/scheme.txt");
		FileProcess.write("====================此处打印丢弃信息:===========================",bw);
		for(int i=0;i<migrationList.size();i++){
			int i_old1=migrationList.get(i).getI_old();
			int j_old1=migrationList.get(i).getJ_old();
			int i_new1=migrationList.get(i).getI_new_from();
			int j_new1=migrationList.get(i).getJ_new_from();
			
			for(int j=0;j<migrationList.get(i).getR_discard_list().size();j++){
				FileProcess.write("旧节点：["+i_old1+","+j_old1+"] 即是新节点：["+i_new1+","+j_new1+"] 需丢弃Ｒ "+
						migrationList.get(i).getR_discard_list().get(j).getBegin()+" -> "+
						migrationList.get(i).getR_discard_list().get(j).getEnd()
						,bw);
			}
			for(int k=0;k<migrationList.get(i).getS_discard_list().size();k++){
				FileProcess.write("旧节点： ["+i_old1+","+j_old1+"] 即是新节点：["+i_new1+","+j_new1+"] 需丢弃S "+
						migrationList.get(i).getS_discard_list().get(k).getBegin()+" -> "+
						migrationList.get(i).getS_discard_list().get(k).getEnd()
						,bw);
			}
		}
		FileProcess.write("", bw);
		FileProcess.close(bw);
	}
	
	public static void print_discard(
			Vector<Mig_Info> migrationList) {
		// TODO Auto-generated method stub
		//System.out.println("====================此处打印丢弃信息:===========================");
		for(int i=0;i<migrationList.size();i++){
			int i_old1=migrationList.get(i).getI_old();
			int j_old1=migrationList.get(i).getJ_old();
			int i_new1=migrationList.get(i).getI_new_from();
			int j_new1=migrationList.get(i).getJ_new_from();
			
			for(int j=0;j<migrationList.get(i).getR_discard_list().size();j++){
				/*System.out.println("旧节点：["+i_old1+","+j_old1+"] 即是新节点：["+i_new1+","+j_new1+"] 需丢弃Ｒ "+
						migrationList.get(i).getR_discard_list().get(j).getBegin()+" -> "+
						migrationList.get(i).getR_discard_list().get(j).getEnd()
						);*/
			}
			for(int k=0;k<migrationList.get(i).getS_discard_list().size();k++){
				//System.out.println("旧节点： ["+i_old1+","+j_old1+"] 即是新节点：["+i_new1+","+j_new1+"] 需丢弃S "+
						//migrationList.get(i).getS_discard_list().get(k).getBegin()+" -> "+
						//migrationList.get(i).getS_discard_list().get(k).getEnd()
						//);
			}
		}
	}
	
	public static void print_migration_plan(Vector<Mig_Info> migrationList,int changeIndex){
		BufferedWriter bw=FileProcess.getWriter("Experiment/plan2.txt");
		FileProcess.write(changeIndex+" ====================此处打印迁移信息:===========================",bw);
		for(int i=0;i<migrationList.size();i++){

			int i_old=migrationList.get(i).getI_old();
			int j_old=migrationList.get(i).getJ_old();
			int i_new=migrationList.get(i).getI_new_from();
			int j_new=migrationList.get(i).getJ_new_from();
			FileProcess.write("旧节点:["+i_old+","+j_old+"]　即是新节点:["+i_new+","+j_new+"] ", bw);
			for(int j=0;j<migrationList.get(i).getR_discard_list().size();j++){
				FileProcess.write("旧节点:["+i_old+","+j_old+"] 即是新节点:["+i_new+","+j_new+"] 丢弃了Ｒ中的("+migrationList.get(i).getR_discard_list().get(j).getBegin()+"->"+migrationList.get(i).getR_discard_list().get(j).getEnd()+")", bw);
			}
			for(int k=0;k<migrationList.get(i).getS_discard_list().size();k++){
				FileProcess.write("旧节点:["+i_old+","+j_old+"] 即是新节点:["+i_new+","+j_new+"] 丢弃了S中的("+migrationList.get(i).getS_discard_list().get(k).getBegin()+"->"+migrationList.get(i).getS_discard_list().get(k).getEnd()+")", bw);
			}
			FileProcess.write("迁移到的目的节点是：", bw);
			for(int l=0;l<migrationList.get(i).getR_i_j_list().size();l++){
				int to_new_node_i=migrationList.get(i).getR_i_j_list().get(l).getI_new();
				int to_new_node_j=migrationList.get(i).getR_i_j_list().get(l).getJ_new();
				if(migrationList.get(i).getR_i_j_list().get(l).getBe_send_list().size()>0){
					FileProcess.write("\t 发往Ｒ－－－》新节点: ["+to_new_node_i+","+to_new_node_j+"]", bw);
					for(int n=0;n<migrationList.get(i).getR_i_j_list().get(l).getBe_send_list().size();n++){
						FileProcess.write("\t\t ["+migrationList.get(i).getR_i_j_list().get(l).getBe_send_list().get(n).getBegin()+","+migrationList.get(i).getR_i_j_list().get(l).getBe_send_list().get(n).getEnd()+"]"+" isUnregular? "+migrationList.get(i).getR_i_j_list().get(l).getBe_send_list().get(n).isUnregular(), bw);
					}
				}
			}
			for(int l=0;l<migrationList.get(i).getS_i_j_list().size();l++){
				int to_new_node_i=migrationList.get(i).getS_i_j_list().get(l).getI_new();
				int to_new_node_j=migrationList.get(i).getS_i_j_list().get(l).getJ_new();
				if(migrationList.get(i).getS_i_j_list().get(l).getBe_send_list().size()>0){
					FileProcess.write("\t 发往S－－－》新节点: ["+to_new_node_i+","+to_new_node_j+"]", bw);
					for(int n=0;n<migrationList.get(i).getS_i_j_list().get(l).getBe_send_list().size();n++){
						FileProcess.write("\t\t ["+migrationList.get(i).getS_i_j_list().get(l).getBe_send_list().get(n).getBegin()+","+migrationList.get(i).getS_i_j_list().get(l).getBe_send_list().get(n).getEnd()+"]"+" isUnregular? "+migrationList.get(i).getS_i_j_list().get(l).getBe_send_list().get(n).isUnregular(), bw);
					}
				}
			}
			FileProcess.write("===============================================", bw);
		}
	}
	
}

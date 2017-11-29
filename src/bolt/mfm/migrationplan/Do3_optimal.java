package bolt.mfm.migrationplan;

import java.util.Vector;



public class Do3_optimal {

	public static void gen_migration_plan(Vector<Old_New_Relation_optimal> relationList_optimal) {
		// TODO Auto-generated method stub
		// *********预处理，写入needlist 内容
		preprocessing_for_migration_inti_needlist();
		// *********映射成矩阵
		preprocessing_for_map_node_list();

		Processing_discard_for_each_node();
		
		Tools_optimal.print_need(relationList_optimal);
		Tools_optimal.print_discard(Para.migrationList);
		
		int col_num_old = Para.oldnodes_optimal[0].length;
		int row_num_old = Para.oldnodes_optimal.length;
		int col_num_new = Para.newnodes_optimal[0].length;
		int row_num_new = Para.newnodes_optimal.length;
		//第一步：将新架构的（行R）调整正相同
		for(int row_new = 0; row_new < row_num_new; row_new++){
			  for (int col_new = 0; col_new < col_num_new; col_new++) {
				  if (Para.newnodes_optimal[row_new][col_new].isActivity()) {
					int i = Para.newnodes_optimal[row_new][col_new].getPosition_in_list();
					Mig_Info mig_info = new Mig_Info();
					// *********写入该节点位置
					mig_info.setI_new_from(Para.relationList_optimal.get(i).getI_new());
					mig_info.setJ_new_from(Para.relationList_optimal.get(i).getJ_new());
					// *********写入对应的源节点位置
					mig_info.setI_old(Para.relationList_optimal.get(i).getI_old());
					mig_info.setJ_old(Para.relationList_optimal.get(i).getJ_old());

					Vector<To_I_J_B_E> r_toijbelist = new Vector<To_I_J_B_E>();

					double r_had_begin = Para.relationList_optimal.get(i).getR_had_new().getBegin();
					double r_had_end = Para.relationList_optimal.get(i).getR_had_new().getEnd();
					
				  for (int col_new1 = 0; col_new1 < col_num_new; col_new1++) {//对于这一行的所有节点
						if (col_new == col_new1 || !Para.newnodes_optimal[row_new][col_new].isActivity()){continue;}

						int j = Para.newnodes_optimal[row_new][col_new1].getPosition_in_list();

						To_I_J_B_E to_r_ijbe = new To_I_J_B_E(); // 每个目的节点有一个
						to_r_ijbe.setI_new(Para.relationList_optimal.get(j).getI_new());
						to_r_ijbe.setJ_new(Para.relationList_optimal.get(j).getJ_new());
						to_r_ijbe.setI_old(Para.relationList_optimal.get(j).getI_old());
						to_r_ijbe.setJ_old(Para.relationList_optimal.get(j).getJ_old());
						Vector<Begin_End> be_r_list = new Vector<Begin_End>();

						for (int k = 0; k < Para.relationList_optimal.get(j).getR_need_list().size(); k++) {// 可能需要很多
							// 1.计算R
							double r_need_begin = Para.relationList_optimal.get(j).getR_need_list().get(k).getBegin();
							double r_need_end = Para.relationList_optimal.get(j).getR_need_list().get(k).getEnd();
							if (r_need_begin >= r_had_end
									|| r_need_end <= r_had_begin) {// 关系1: 不相交
								// continue;
							} else if (r_need_begin >= r_had_begin
									&& r_need_end <= r_had_end) {// 关系2: had 全包含
																	// need
								// 更新迁移计划 --》
								Begin_End r_be = new Begin_End();
								r_be.setBegin(r_need_begin);
								r_be.setEnd(r_need_end);
								be_r_list.add(r_be);

								// 删除需求 --》在relationlist 中 该need 段
								Para.relationList_optimal.get(j).getR_need_list()
										.remove(k);
								k--;
							} else if (r_need_begin <= r_had_begin
									&& r_need_end >= r_had_end) {// 关系3: need
																	// 全包含 had
								Begin_End r_be = new Begin_End();
								r_be.setBegin(r_had_begin);
								r_be.setEnd(r_had_end);
								be_r_list.add(r_be);

								Begin_End r_be_1 = new Begin_End();
								r_be_1.setBegin(r_need_begin);
								r_be_1.setEnd(r_had_begin);
								Begin_End r_be_2 = new Begin_End();
								r_be_2.setBegin(r_had_end);
								r_be_2.setEnd(r_need_end);

								Para.relationList_optimal.get(j).getR_need_list().remove(k);

								Para.relationList_optimal.get(j).getR_need_list().add(r_be_1);
								Para.relationList_optimal.get(j).getR_need_list().add(r_be_2);
								k--;
							} else {// 关系4: 部分的包含
								Begin_End r_be = new Begin_End();
								Begin_End r_be1 = new Begin_End();
								if (r_need_begin >= r_had_begin&& r_need_end >= r_had_end) {
									r_be.setBegin(r_need_begin);
									r_be.setEnd(r_had_end);
									be_r_list.add(r_be);

									r_be1.setBegin(r_had_end);
									r_be1.setEnd(r_need_end);

									Para.relationList_optimal.get(j).getR_need_list().remove(k);

									Para.relationList_optimal.get(j).getR_need_list().add(r_be1);
								} else {
									r_be.setBegin(r_had_begin);
									r_be.setEnd(r_need_end);
									be_r_list.add(r_be);

									r_be1.setBegin(r_need_begin);
									r_be1.setEnd(r_had_begin);

									Para.relationList_optimal.get(j).getR_need_list().remove(k);

									Para.relationList_optimal.get(j).getR_need_list().add(r_be1);
								}
								k--;
							}
						}
						to_r_ijbe.setBe_send_list(be_r_list);
						r_toijbelist.add(to_r_ijbe);
					}
					// 打印为了不报错 添加丢弃list 和 S
					Vector<Begin_End> r_discard_be_List = new Vector<Begin_End>();
					Vector<Begin_End> s_discard_be_List = new Vector<Begin_End>();
					Vector<To_I_J_B_E> s_toijbelist = new Vector<To_I_J_B_E>();

					mig_info.setR_discard_list(r_discard_be_List);
					mig_info.setS_discard_list(s_discard_be_List);
					mig_info.setS_i_j_list(s_toijbelist);

					mig_info.setR_i_j_list(r_toijbelist);
					Para.migrationList.add(mig_info);
					//System.out.println("装载结果体R：" + Para.migrationList.size());
			  }
				  
			}
			  }
		
		//第一步：将新架构的（列S）调整正相同
		
		 for (int col_new = 0; col_new < col_num_new; col_new++) {
			for (int row_new = 0; row_new < row_num_new; row_new++) {
				if (Para.newnodes_optimal[row_new][col_new].isActivity()) {
					int i = Para.newnodes_optimal[row_new][col_new].getPosition_in_list();
					Mig_Info mig_info = new Mig_Info();
					// *********写入该节点位置
					mig_info.setI_new_from(Para.relationList_optimal.get(i).getI_new());
					mig_info.setJ_new_from(Para.relationList_optimal.get(i).getJ_new());
					// *********写入对应的源节点位置
					mig_info.setI_old(Para.relationList_optimal.get(i).getI_old());
					mig_info.setJ_old(Para.relationList_optimal.get(i).getJ_old());

					double s_had_begin = Para.relationList_optimal.get(i).getS_had_new().getBegin();
					double s_had_end = Para.relationList_optimal.get(i).getS_had_new().getEnd();

					Vector<To_I_J_B_E> s_toijbelist = new Vector<To_I_J_B_E>();
					
				for (int row_new1 = 0; row_new1 < row_num_new; row_new1++) {
					if (row_new == row_new1 || !Para.newnodes_optimal[row_new][col_new].isActivity()) {continue;}

					int j = Para.newnodes_optimal[row_new1][col_new].getPosition_in_list();

					To_I_J_B_E to_s_ijbe = new To_I_J_B_E(); // 每个目的节点有一个
					to_s_ijbe.setI_new(Para.relationList_optimal.get(j).getI_new());
					to_s_ijbe.setJ_new(Para.relationList_optimal.get(j).getJ_new());
					to_s_ijbe.setI_old(Para.relationList_optimal.get(j).getI_old());
					to_s_ijbe.setJ_old(Para.relationList_optimal.get(j).getJ_old());
					Vector<Begin_End> be_s_list = new Vector<Begin_End>();

					for (int k = 0; k < Para.relationList_optimal.get(j)
							.getS_need_list().size(); k++) {// 可能需要很多
						double s_need_begin = Para.relationList_optimal.get(j)
								.getS_need_list().get(k).getBegin();
						double s_need_end = Para.relationList_optimal.get(j)
								.getS_need_list().get(k).getEnd();

						if (s_need_begin >= s_had_end
								|| s_need_end <= s_had_begin) {// 关系1: 不相交
							// continue;
						} else if (s_need_begin >= s_had_begin
								&& s_need_end <= s_had_end) {// 关系2: had 全包含
																// need
							// 更新迁移计划 --》
							Begin_End s_be = new Begin_End();
							s_be.setBegin(s_need_begin);
							s_be.setEnd(s_need_end);
							be_s_list.add(s_be);

							// 删除需求 --》在relationlist 中 该need 段
							Para.relationList_optimal.get(j).getS_need_list()
									.remove(k);
							k--;
						} else if (s_need_begin <= s_had_begin
								&& s_need_end >= s_had_end) {// 关系3: need
																// 全包含 had
							Begin_End s_be = new Begin_End();
							s_be.setBegin(s_had_begin);
							s_be.setEnd(s_had_end);
							be_s_list.add(s_be);

							Begin_End s_be_1 = new Begin_End();
							s_be_1.setBegin(s_need_begin);
							s_be_1.setEnd(s_had_begin);
							Begin_End s_be_2 = new Begin_End();
							s_be_2.setBegin(s_had_end);
							s_be_2.setEnd(s_need_end);

							Para.relationList_optimal.get(j).getS_need_list()
									.remove(k);

							Para.relationList_optimal.get(j).getS_need_list()
									.add(s_be_1);
							Para.relationList_optimal.get(j).getS_need_list()
									.add(s_be_2);
							k--;
						} else {// 关系4: 部分的包含
							Begin_End s_be = new Begin_End();
							Begin_End s_be1 = new Begin_End();
							if (s_need_begin >= s_had_begin
									&& s_need_end >= s_had_end) {
								s_be.setBegin(s_need_begin);
								s_be.setEnd(s_had_end);
								be_s_list.add(s_be);

								s_be1.setBegin(s_had_end);
								s_be1.setEnd(s_need_end);

								// System.out.println("测试事实上司和111111111111111："+s_need_end+" , "+s_had_end);

								Para.relationList_optimal.get(j).getS_need_list()
										.remove(k);

								Para.relationList_optimal.get(j).getS_need_list()
										.add(s_be1);

							} else {
								s_be.setBegin(s_had_begin);
								s_be.setEnd(s_need_end);
								be_s_list.add(s_be);

								s_be1.setBegin(s_need_begin);
								s_be1.setEnd(s_had_begin);

								Para.relationList_optimal.get(j).getS_need_list()
										.remove(k);

								Para.relationList_optimal.get(j).getS_need_list()
										.add(s_be1);
								// System.out.println("测试事实上司和2222222222222222："+s_need_begin+" , "+s_had_begin);
							}
							// System.out.println( s_had_begin +" , "+
							// s_had_end +" , "+ s_need_begin +" , "+
							// s_need_end);
							// System.err.println("放入3：["+s_be1.getBegin()+","+s_be1.getEnd()+"]");
							k--;
						}
						// System.out.println("s---------s_had: "+r_had_begin+"->"+s_had_end+"s_need: "+s_need_begin+"->"+s_need_end);
					}
					to_s_ijbe.setBe_send_list(be_s_list);
					s_toijbelist.add(to_s_ijbe);
				
				}
				// 打印为了不报错 添加丢弃list 和 R
				Vector<Begin_End> r_discard_be_List = new Vector<Begin_End>();
				Vector<Begin_End> s_discard_be_List = new Vector<Begin_End>();
				Vector<To_I_J_B_E> r_toijbelist = new Vector<To_I_J_B_E>();
				mig_info.setR_discard_list(r_discard_be_List);
				mig_info.setS_discard_list(s_discard_be_List);
				mig_info.setR_i_j_list(r_toijbelist);

				mig_info.setS_i_j_list(s_toijbelist);
				Para.migrationList.add(mig_info);
				//System.out.println("装载结果体S：" + Para.migrationList.size());
		
			}
			}}
		
		//第二步：用旧架构将新架构的（R）补充完整 --> 第一列的R即可
		for(int row_old = 0; row_old <row_num_old; row_old++){
			if (Para.oldnodes_optimal[row_old][0].isActivity()) {
				int i = Para.oldnodes_optimal[row_old][0].getPosition_in_list();

				Mig_Info mig_info = new Mig_Info();
				// *********写入该节点位置
				mig_info.setI_new_from(Para.relationList_optimal.get(i).getI_new());
				mig_info.setJ_new_from(Para.relationList_optimal.get(i).getJ_new());
				// *********写入对应的源节点位置
				mig_info.setI_old(Para.relationList_optimal.get(i).getI_old());
				mig_info.setJ_old(Para.relationList_optimal.get(i).getJ_old());

				Vector<To_I_J_B_E> r_toijbelist = new Vector<To_I_J_B_E>();

				double r_had_begin = Para.relationList_optimal.get(i).getR_had_new().getBegin();
				double r_had_end = Para.relationList_optimal.get(i).getR_had_new().getEnd();

				for(int row_new = 0; row_new < row_num_new; row_new++){
				  for (int col_new = 0; col_new < col_num_new; col_new++) {
					if (!Para.newnodes_optimal[row_new][col_new].isActivity()){continue;}

					int j = Para.newnodes_optimal[row_new][col_new].getPosition_in_list();

					To_I_J_B_E to_r_ijbe = new To_I_J_B_E(); // 每个目的节点有一个
					to_r_ijbe.setI_new(Para.relationList_optimal.get(j).getI_new());
					to_r_ijbe.setJ_new(Para.relationList_optimal.get(j).getJ_new());
					to_r_ijbe.setI_old(Para.relationList_optimal.get(j).getI_old());
					to_r_ijbe.setJ_old(Para.relationList_optimal.get(j).getJ_old());
					Vector<Begin_End> be_r_list = new Vector<Begin_End>();

					for (int k = 0; k < Para.relationList_optimal.get(j)
							.getR_need_list().size(); k++) {// 可能需要很多
						// 1.计算R
						double r_need_begin = Para.relationList_optimal.get(j)
								.getR_need_list().get(k).getBegin();
						double r_need_end = Para.relationList_optimal.get(j)
								.getR_need_list().get(k).getEnd();
						if (r_need_begin >= r_had_end
								|| r_need_end <= r_had_begin) {// 关系1: 不相交
							// continue;
						} else if (r_need_begin >= r_had_begin
								&& r_need_end <= r_had_end) {// 关系2: had 全包含
																// need
							// 更新迁移计划 --》
							Begin_End r_be = new Begin_End();
							r_be.setBegin(r_need_begin);
							r_be.setEnd(r_need_end);
							be_r_list.add(r_be);

							// 删除需求 --》在relationlist 中 该need 段
							Para.relationList_optimal.get(j).getR_need_list()
									.remove(k);
							k--;
						} else if (r_need_begin <= r_had_begin
								&& r_need_end >= r_had_end) {// 关系3: need
																// 全包含 had
							Begin_End r_be = new Begin_End();
							r_be.setBegin(r_had_begin);
							r_be.setEnd(r_had_end);
							be_r_list.add(r_be);

							Begin_End r_be_1 = new Begin_End();
							r_be_1.setBegin(r_need_begin);
							r_be_1.setEnd(r_had_begin);
							Begin_End r_be_2 = new Begin_End();
							r_be_2.setBegin(r_had_end);
							r_be_2.setEnd(r_need_end);

							Para.relationList_optimal.get(j).getR_need_list()
									.remove(k);

							Para.relationList_optimal.get(j).getR_need_list()
									.add(r_be_1);
							Para.relationList_optimal.get(j).getR_need_list()
									.add(r_be_2);
							k--;
						} else {// 关系4: 部分的包含
							Begin_End r_be = new Begin_End();
							Begin_End r_be1 = new Begin_End();
							if (r_need_begin >= r_had_begin
									&& r_need_end >= r_had_end) {
								r_be.setBegin(r_need_begin);
								r_be.setEnd(r_had_end);
								be_r_list.add(r_be);

								r_be1.setBegin(r_had_end);
								r_be1.setEnd(r_need_end);

								Para.relationList_optimal.get(j).getR_need_list()
										.remove(k);

								Para.relationList_optimal.get(j).getR_need_list()
										.add(r_be1);
							} else {
								r_be.setBegin(r_had_begin);
								r_be.setEnd(r_need_end);
								be_r_list.add(r_be);

								r_be1.setBegin(r_need_begin);
								r_be1.setEnd(r_had_begin);

								Para.relationList_optimal.get(j).getR_need_list()
										.remove(k);

								Para.relationList_optimal.get(j).getR_need_list()
										.add(r_be1);
							}
							k--;
						}
					}
					to_r_ijbe.setBe_send_list(be_r_list);
					r_toijbelist.add(to_r_ijbe);
				}
				}
				// 打印为了不报错 添加丢弃list 和 S
				Vector<Begin_End> r_discard_be_List = new Vector<Begin_End>();
				Vector<Begin_End> s_discard_be_List = new Vector<Begin_End>();
				Vector<To_I_J_B_E> s_toijbelist = new Vector<To_I_J_B_E>();

				mig_info.setR_discard_list(r_discard_be_List);
				mig_info.setS_discard_list(s_discard_be_List);
				mig_info.setS_i_j_list(s_toijbelist);

				mig_info.setR_i_j_list(r_toijbelist);
				Para.migrationList.add(mig_info);
				//System.out.println("装载结果体R：" + Para.migrationList.size());
			}
		
		}
		
		//第二步：用旧架构将新架构的（行S）补充完整 --> 第一行的S即可
		for(int col_old = 0; col_old < col_num_old; col_old++){
			if (Para.oldnodes_optimal[0][col_old].isActivity()) {
				int i = Para.oldnodes_optimal[0][col_old].getPosition_in_list();

				Mig_Info mig_info = new Mig_Info();
				// *********写入该节点位置
				mig_info.setI_new_from(Para.relationList_optimal.get(i).getI_new());
				mig_info.setJ_new_from(Para.relationList_optimal.get(i).getJ_new());
				// *********写入对应的源节点位置
				mig_info.setI_old(Para.relationList_optimal.get(i).getI_old());
				mig_info.setJ_old(Para.relationList_optimal.get(i).getJ_old());

				double s_had_begin = Para.relationList_optimal.get(i).getS_had_new().getBegin();
				double s_had_end = Para.relationList_optimal.get(i).getS_had_new().getEnd();

				Vector<To_I_J_B_E> s_toijbelist = new Vector<To_I_J_B_E>();
			 for (int col_new = 0; col_new < col_num_new; col_new++) {
				for (int row_new = 0; row_new < row_num_new; row_new++) {

					if (!Para.newnodes_optimal[row_new][col_new].isActivity()) {continue;}

					int j = Para.newnodes_optimal[row_new][col_new].getPosition_in_list();

					To_I_J_B_E to_s_ijbe = new To_I_J_B_E(); // 每个目的节点有一个
					to_s_ijbe.setI_new(Para.relationList_optimal.get(j).getI_new());
					to_s_ijbe.setJ_new(Para.relationList_optimal.get(j).getJ_new());
					to_s_ijbe.setI_old(Para.relationList_optimal.get(j).getI_old());
					to_s_ijbe.setJ_old(Para.relationList_optimal.get(j).getJ_old());
					Vector<Begin_End> be_s_list = new Vector<Begin_End>();

					for (int k = 0; k < Para.relationList_optimal.get(j)
							.getS_need_list().size(); k++) {// 可能需要很多
						double s_need_begin = Para.relationList_optimal.get(j)
								.getS_need_list().get(k).getBegin();
						double s_need_end = Para.relationList_optimal.get(j)
								.getS_need_list().get(k).getEnd();

						if (s_need_begin >= s_had_end
								|| s_need_end <= s_had_begin) {// 关系1: 不相交
							// continue;
						} else if (s_need_begin >= s_had_begin
								&& s_need_end <= s_had_end) {// 关系2: had 全包含
																// need
							// 更新迁移计划 --》
							Begin_End s_be = new Begin_End();
							s_be.setBegin(s_need_begin);
							s_be.setEnd(s_need_end);
							be_s_list.add(s_be);

							// 删除需求 --》在relationlist 中 该need 段
							Para.relationList_optimal.get(j).getS_need_list()
									.remove(k);
							// System.err.println("放入1：["+s_be.getBegin()+","+s_be.getEnd()+"]");
							k--;
						} else if (s_need_begin <= s_had_begin
								&& s_need_end >= s_had_end) {// 关系3: need
																// 全包含 had
							Begin_End s_be = new Begin_End();
							s_be.setBegin(s_had_begin);
							s_be.setEnd(s_had_end);
							be_s_list.add(s_be);

							Begin_End s_be_1 = new Begin_End();
							s_be_1.setBegin(s_need_begin);
							s_be_1.setEnd(s_had_begin);
							Begin_End s_be_2 = new Begin_End();
							s_be_2.setBegin(s_had_end);
							s_be_2.setEnd(s_need_end);

							Para.relationList_optimal.get(j).getS_need_list()
									.remove(k);

							// System.err.println("放入2：["+s_be_1.getBegin()+","+s_be_1.getEnd()+"]");
							// System.err.println("放入2：["+s_be_2.getBegin()+","+s_be_2.getEnd()+"]");

							Para.relationList_optimal.get(j).getS_need_list()
									.add(s_be_1);
							Para.relationList_optimal.get(j).getS_need_list()
									.add(s_be_2);
							k--;
						} else {// 关系4: 部分的包含
							Begin_End s_be = new Begin_End();
							Begin_End s_be1 = new Begin_End();
							if (s_need_begin >= s_had_begin
									&& s_need_end >= s_had_end) {
								s_be.setBegin(s_need_begin);
								s_be.setEnd(s_had_end);
								be_s_list.add(s_be);

								s_be1.setBegin(s_had_end);
								s_be1.setEnd(s_need_end);

								// System.out.println("测试事实上司和111111111111111："+s_need_end+" , "+s_had_end);

								Para.relationList_optimal.get(j).getS_need_list()
										.remove(k);

								Para.relationList_optimal.get(j).getS_need_list()
										.add(s_be1);

							} else {
								s_be.setBegin(s_had_begin);
								s_be.setEnd(s_need_end);
								be_s_list.add(s_be);

								s_be1.setBegin(s_need_begin);
								s_be1.setEnd(s_had_begin);

								Para.relationList_optimal.get(j).getS_need_list()
										.remove(k);

								Para.relationList_optimal.get(j).getS_need_list()
										.add(s_be1);
								// System.out.println("测试事实上司和2222222222222222："+s_need_begin+" , "+s_had_begin);
							}
							// System.out.println( s_had_begin +" , "+
							// s_had_end +" , "+ s_need_begin +" , "+
							// s_need_end);
							// System.err.println("放入3：["+s_be1.getBegin()+","+s_be1.getEnd()+"]");
							k--;
						}
						// System.out.println("s---------s_had: "+r_had_begin+"->"+s_had_end+"s_need: "+s_need_begin+"->"+s_need_end);
					}
					to_s_ijbe.setBe_send_list(be_s_list);
					s_toijbelist.add(to_s_ijbe);
				}
				}
				// 打印为了不报错 添加丢弃list 和 R
				Vector<Begin_End> r_discard_be_List = new Vector<Begin_End>();
				Vector<Begin_End> s_discard_be_List = new Vector<Begin_End>();
				Vector<To_I_J_B_E> r_toijbelist = new Vector<To_I_J_B_E>();
				mig_info.setR_discard_list(r_discard_be_List);
				mig_info.setS_discard_list(s_discard_be_List);
				mig_info.setR_i_j_list(r_toijbelist);

				mig_info.setS_i_j_list(s_toijbelist);
				Para.migrationList.add(mig_info);
				//System.out.println("装载结果体S：" + Para.migrationList.size());
			}
		
		}
		
		//少在  lack_row_or_col  最后 有 last_num 个节点
		//第三步：补全少在列上的R   按R平均分于行上-->S列会产生最后一列的碎片-->将最后一列减少节点个数-->将最后一列上的R聚集到存在的最后一列的节点上（s是无关的，因为s在最后一列上是复制的）  
		//lack_row_or_col = "col"
		if(Para.lack_row_or_col.equals("col")){//
			//将需求压倒最后一列
			double R_compensation_begin = (double)(Para.last_num)/row_num_new;
			double R_compensation_end = 1;
			
			double R_compensation_range = R_compensation_end - R_compensation_begin;
			double r_compensation_length = R_compensation_range/Para.last_num;
			
			//先把所有应有作为需求list
			Vector<Begin_End> re_assign_R_list = new Vector<Begin_End>();
			for(int move_row = Para.last_num-1; move_row<row_num_new;move_row++){
				for(int hl=0;hl<Para.newnodes_optimal[move_row][0].getR_list().size();hl++){
					Begin_End be = new Begin_End();
					double this_move_row_begin = Para.newnodes_optimal[move_row][0].getR_list().get(hl).getBegin();
					double this_move_row_end = Para.newnodes_optimal[move_row][0].getR_list().get(hl).getEnd();
					be.setBegin(this_move_row_begin);
					be.setEnd(this_move_row_end);
					re_assign_R_list.add(be);
				}}
			
			for(int last_row=0; last_row<Para.last_num;last_row++){
				double countrange=0;
				for(int i=0; i<re_assign_R_list.size();i++){
					if(countrange<r_compensation_length){
					Begin_End sub_be_remain = null;
					Begin_End be_put = new Begin_End();
					double this_move_row_begin_in_list = re_assign_R_list.get(i).getBegin();
					double this_move_row_end_in_list = re_assign_R_list.get(i).getEnd();
					double sub_range = this_move_row_end_in_list - this_move_row_begin_in_list;
					if(countrange+sub_range<r_compensation_length){
						be_put.setBegin(this_move_row_begin_in_list);
						be_put.setEnd(this_move_row_end_in_list);
						
						countrange+=sub_range;
					}else{
						sub_be_remain = new Begin_End();
						double sub_sub_range = r_compensation_length - countrange;
						be_put.setBegin(this_move_row_begin_in_list);
						be_put.setEnd(this_move_row_begin_in_list+sub_sub_range);
						Begin_End be_remain = new Begin_End();
						be_remain.setBegin(this_move_row_begin_in_list+sub_sub_range);
						be_remain.setEnd(this_move_row_end_in_list);
						countrange+=sub_sub_range;
					}
					be_put.setUnregular(true);
					//将其压到相应新节点的需求list里
					int position_in_relationList = Para.newnodes_optimal[last_row][col_num_new-1].getPosition_in_list();
					Para.relationList_optimal.get(position_in_relationList).getR_need_list().add(be_put);
					re_assign_R_list.remove(0);
					i--;
					if(sub_be_remain!=null){re_assign_R_list.add(sub_be_remain);}
				}}
			}
			//用第一列的R将最后一列的R补偿
			for(int row_old = 0; row_old <row_num_old; row_old++){
				if (Para.oldnodes_optimal[row_old][0].isActivity()) {
					int i = Para.oldnodes_optimal[row_old][0].getPosition_in_list();

					Mig_Info mig_info = new Mig_Info();
					// *********写入该节点位置
					mig_info.setI_new_from(Para.relationList_optimal.get(i).getI_new());
					mig_info.setJ_new_from(Para.relationList_optimal.get(i).getJ_new());
					// *********写入对应的源节点位置
					mig_info.setI_old(Para.relationList_optimal.get(i).getI_old());
					mig_info.setJ_old(Para.relationList_optimal.get(i).getJ_old());

					Vector<To_I_J_B_E> r_toijbelist = new Vector<To_I_J_B_E>();

					double r_had_begin = Para.relationList_optimal.get(i).getR_had_new().getBegin();
					double r_had_end = Para.relationList_optimal.get(i).getR_had_new().getEnd();

					for(int row_new = 0; row_new < row_num_new; row_new++){
						
							if (!Para.newnodes_optimal[row_new][col_num_new-1].isActivity()){continue;}

							int j = Para.newnodes_optimal[row_new][col_num_new-1].getPosition_in_list();

							To_I_J_B_E to_r_ijbe = new To_I_J_B_E(); // 每个目的节点有一个
							to_r_ijbe.setI_new(Para.relationList_optimal.get(j).getI_new());
							to_r_ijbe.setJ_new(Para.relationList_optimal.get(j).getJ_new());
							to_r_ijbe.setI_old(Para.relationList_optimal.get(j).getI_old());
							to_r_ijbe.setJ_old(Para.relationList_optimal.get(j).getJ_old());
							Vector<Begin_End> be_r_list = new Vector<Begin_End>();

							for (int k = 0; k < Para.relationList_optimal.get(j).getR_need_list().size(); k++) {// 可能需要很多
								// 1.计算R
								if(Para.relationList_optimal.get(j).getR_need_list().get(k).isUnregular()){
								double r_need_begin = Para.relationList_optimal.get(j).getR_need_list().get(k).getBegin();
								double r_need_end = Para.relationList_optimal.get(j).getR_need_list().get(k).getEnd();
								if (r_need_begin >= r_had_end|| r_need_end <= r_had_begin) {// 关系1: 不相交
									// continue;
								} else if (r_need_begin >= r_had_begin&& r_need_end <= r_had_end) {// 关系2: had 全包含
																		// need
									// 更新迁移计划 --》
									Begin_End r_be = new Begin_End();
									r_be.setBegin(r_need_begin);
									r_be.setEnd(r_need_end);
									r_be.setUnregular(true);
									be_r_list.add(r_be);

									// 删除需求 --》在relationlist 中 该need 段
									Para.relationList_optimal.get(j).getR_need_list().remove(k);
									k--;
								} else if (r_need_begin <= r_had_begin
										&& r_need_end >= r_had_end) {// 关系3: need
																		// 全包含 had
									Begin_End r_be = new Begin_End();
									r_be.setBegin(r_had_begin);
									r_be.setEnd(r_had_end);
									r_be.setUnregular(true);
									be_r_list.add(r_be);

									Begin_End r_be_1 = new Begin_End();
									r_be_1.setBegin(r_need_begin);
									r_be_1.setEnd(r_had_begin);
									Begin_End r_be_2 = new Begin_End();
									r_be_2.setBegin(r_had_end);
									r_be_2.setEnd(r_need_end);
									
									r_be_1.setUnregular(true);
									r_be_2.setUnregular(true);

									Para.relationList_optimal.get(j).getR_need_list().remove(k);

									Para.relationList_optimal.get(j).getR_need_list().add(r_be_1);
									Para.relationList_optimal.get(j).getR_need_list().add(r_be_2);
									k--;
								} else {// 关系4: 部分的包含
									Begin_End r_be = new Begin_End();
									Begin_End r_be1 = new Begin_End();
									if (r_need_begin >= r_had_begin
											&& r_need_end >= r_had_end) {
										r_be.setBegin(r_need_begin);
										r_be.setEnd(r_had_end);
										r_be.setUnregular(true);
										be_r_list.add(r_be);

										r_be1.setBegin(r_had_end);
										r_be1.setEnd(r_need_end);

										Para.relationList_optimal.get(j).getR_need_list().remove(k);

										r_be1.setUnregular(true);
										Para.relationList_optimal.get(j).getR_need_list().add(r_be1);
									} else {
										r_be.setBegin(r_had_begin);
										r_be.setEnd(r_need_end);
										r_be.setUnregular(true);
										be_r_list.add(r_be);

										r_be1.setBegin(r_need_begin);
										r_be1.setEnd(r_had_begin);

										Para.relationList_optimal.get(j).getR_need_list().remove(k);
										r_be1.setUnregular(true);
										Para.relationList_optimal.get(j).getR_need_list().add(r_be1);
									}
									k--;
								}
							}
								}
							to_r_ijbe.setBe_send_list(be_r_list);
							r_toijbelist.add(to_r_ijbe);
						}
						// 打印为了不报错 添加丢弃list 和 S
						Vector<Begin_End> r_discard_be_List = new Vector<Begin_End>();
						Vector<Begin_End> s_discard_be_List = new Vector<Begin_End>();
						Vector<To_I_J_B_E> s_toijbelist = new Vector<To_I_J_B_E>();

						mig_info.setR_discard_list(r_discard_be_List);
						mig_info.setS_discard_list(s_discard_be_List);
						mig_info.setS_i_j_list(s_toijbelist);

						mig_info.setR_i_j_list(r_toijbelist);
						Para.migrationList.add(mig_info);
						//System.out.println("装载结果体R：" + Para.migrationList.size());
				}
				}
		}
		
		//第三步：补全少在行上的S   按S平均分于行上-->R列会产生最后一行的碎片-->将最后一行减少节点个数-->将最后一行上的S聚集到存在的最后一行的节点上（R是无关的，因为R在最后一列上是复制的）  
		//lack_row_or_col = "row"
		if(Para.lack_row_or_col.equals("row")){//
			double s_compensation_begin = (double)(Para.last_num)/col_num_new;
			double s_compensation_end = 1;
			
			double s_compensation_range = s_compensation_end - s_compensation_begin;
			double s_compensation_length = s_compensation_range/Para.last_num;
			
			//先把所有应有作为需求list
			Vector<Begin_End> re_assign_S_list = new Vector<Begin_End>();
			for(int move_col = Para.last_num-1; move_col<col_num_new;move_col++){
				for(int hl=0;hl<Para.newnodes_optimal[0][move_col].getS_list().size();hl++){
					Begin_End be = new Begin_End();
					double this_move_col_begin = Para.newnodes_optimal[0][move_col].getS_list().get(hl).getBegin();
					double this_move_col_end = Para.newnodes_optimal[0][move_col].getS_list().get(hl).getEnd();
					be.setBegin(this_move_col_begin);
					be.setEnd(this_move_col_end);
					re_assign_S_list.add(be);
				}}
			
			for(int last_col=0; last_col<Para.last_num;last_col++){
				double countrange=0;
				for(int i=0; i<re_assign_S_list.size();i++){
					if(countrange<s_compensation_length){
					Begin_End sub_be_remain = null;
					Begin_End be_put = new Begin_End();
					double this_move_col_begin_in_list = re_assign_S_list.get(i).getBegin();
					double this_move_col_end_in_list = re_assign_S_list.get(i).getEnd();
					double sub_range = this_move_col_end_in_list - this_move_col_begin_in_list;
					if(countrange+sub_range<s_compensation_length){
						be_put.setBegin(this_move_col_begin_in_list);
						be_put.setEnd(this_move_col_end_in_list);
						
						countrange+=sub_range;
					}else{
						sub_be_remain = new Begin_End();
						double sub_sub_range = s_compensation_length - countrange;
						be_put.setBegin(this_move_col_begin_in_list);
						be_put.setEnd(this_move_col_begin_in_list+sub_sub_range);
						Begin_End be_remain = new Begin_End();
						be_remain.setBegin(this_move_col_begin_in_list+sub_sub_range);
						be_remain.setEnd(this_move_col_end_in_list);
						countrange+=sub_sub_range;
					}
					be_put.setUnregular(true);
					//将其压到相应新节点的需求list里
					int position_in_relationList = Para.newnodes_optimal[row_num_new-1][last_col].getPosition_in_list();
					Para.relationList_optimal.get(position_in_relationList).getS_need_list().add(be_put);
					re_assign_S_list.remove(0);
					i--;
					if(sub_be_remain!=null){re_assign_S_list.add(sub_be_remain);}
				}}
			}		
			//用第一行的S将最后一行的S补偿
			for(int col_old = 0; col_old < col_num_old; col_old++){
				if (Para.oldnodes_optimal[0][col_old].isActivity()) {
					int i = Para.oldnodes_optimal[0][col_old].getPosition_in_list();

					Mig_Info mig_info = new Mig_Info();
					// *********写入该节点位置
					mig_info.setI_new_from(Para.relationList_optimal.get(i).getI_new());
					mig_info.setJ_new_from(Para.relationList_optimal.get(i).getJ_new());
					// *********写入对应的源节点位置
					mig_info.setI_old(Para.relationList_optimal.get(i).getI_old());
					mig_info.setJ_old(Para.relationList_optimal.get(i).getJ_old());

					double s_had_begin = Para.relationList_optimal.get(i).getS_had_new().getBegin();
					double s_had_end = Para.relationList_optimal.get(i).getS_had_new().getEnd();

					Vector<To_I_J_B_E> s_toijbelist = new Vector<To_I_J_B_E>();
				 for (int col_new = 0; col_new < col_num_new; col_new++) {
						if (!Para.newnodes_optimal[row_num_new-1][col_new].isActivity()) {continue;}//最后一行

						int j = Para.newnodes_optimal[row_num_new-1][col_new].getPosition_in_list();

						To_I_J_B_E to_s_ijbe = new To_I_J_B_E(); // 每个目的节点有一个
						to_s_ijbe.setI_new(Para.relationList_optimal.get(j).getI_new());
						to_s_ijbe.setJ_new(Para.relationList_optimal.get(j).getJ_new());
						to_s_ijbe.setI_old(Para.relationList_optimal.get(j).getI_old());
						to_s_ijbe.setJ_old(Para.relationList_optimal.get(j).getJ_old());
						Vector<Begin_End> be_s_list = new Vector<Begin_End>();

						for (int k = 0; k < Para.relationList_optimal.get(j).getS_need_list().size(); k++) {// 可能需要很多
							
							if(Para.relationList_optimal.get(j).getS_need_list().get(k).isUnregular()){
							double s_need_begin = Para.relationList_optimal.get(j).getS_need_list().get(k).getBegin();
							double s_need_end = Para.relationList_optimal.get(j).getS_need_list().get(k).getEnd();

							if (s_need_begin >= s_had_end|| s_need_end <= s_had_begin) {// 关系1: 不相交
								// continue;
							} else if (s_need_begin >= s_had_begin&& s_need_end <= s_had_end) {// 关系2: had 全包含
								Begin_End s_be = new Begin_End();
								s_be.setBegin(s_need_begin);
								s_be.setEnd(s_need_end);
								s_be.setUnregular(true);
								
								be_s_list.add(s_be);
								// 删除需求 --》在relationlist 中 该need 段
								Para.relationList_optimal.get(j).getS_need_list().remove(k);
								k--;
							} else if (s_need_begin <= s_had_begin && s_need_end >= s_had_end) {// 关系3: need
																	// 全包含 had
								Begin_End s_be = new Begin_End();
								s_be.setBegin(s_had_begin);
								s_be.setEnd(s_had_end);
								s_be.setUnregular(true);
								be_s_list.add(s_be);

								Begin_End s_be_1 = new Begin_End();
								s_be_1.setBegin(s_need_begin);
								s_be_1.setEnd(s_had_begin);
								Begin_End s_be_2 = new Begin_End();
								s_be_2.setBegin(s_had_end);
								s_be_2.setEnd(s_need_end);

								Para.relationList_optimal.get(j).getS_need_list().remove(k);

								s_be_1.setUnregular(true);
								s_be_2.setUnregular(true);
								
								Para.relationList_optimal.get(j).getS_need_list().add(s_be_1);
								Para.relationList_optimal.get(j).getS_need_list().add(s_be_2);
								k--;
							} else {// 关系4: 部分的包含
								Begin_End s_be = new Begin_End();
								Begin_End s_be1 = new Begin_End();
								if (s_need_begin >= s_had_begin&& s_need_end >= s_had_end) {
									s_be.setBegin(s_need_begin);
									s_be.setEnd(s_had_end);
									s_be.setUnregular(true);
									be_s_list.add(s_be);

									s_be1.setBegin(s_had_end);
									s_be1.setEnd(s_need_end);

									Para.relationList_optimal.get(j).getS_need_list().remove(k);

									s_be1.setUnregular(true);
									Para.relationList_optimal.get(j).getS_need_list().add(s_be1);

								} else {
									s_be.setBegin(s_had_begin);
									s_be.setEnd(s_need_end);
									s_be.setUnregular(true);
									be_s_list.add(s_be);

									s_be1.setBegin(s_need_begin);
									s_be1.setEnd(s_had_begin);

									Para.relationList_optimal.get(j).getS_need_list().remove(k);

									s_be1.setUnregular(true);
									Para.relationList_optimal.get(j).getS_need_list().add(s_be1);
								}
								k--;
							}
						}}
						to_s_ijbe.setBe_send_list(be_s_list);
						s_toijbelist.add(to_s_ijbe);
					}
					// 打印为了不报错 添加丢弃list 和 R
					Vector<Begin_End> r_discard_be_List = new Vector<Begin_End>();
					Vector<Begin_End> s_discard_be_List = new Vector<Begin_End>();
					Vector<To_I_J_B_E> r_toijbelist = new Vector<To_I_J_B_E>();
					mig_info.setR_discard_list(r_discard_be_List);
					mig_info.setS_discard_list(s_discard_be_List);
					mig_info.setR_i_j_list(r_toijbelist);

					mig_info.setS_i_j_list(s_toijbelist);
					Para.migrationList.add(mig_info);
					//System.out.println("装载结果体S：" + Para.migrationList.size());
				}
			}
		}
	}

	private static void Processing_discard_for_each_node() {
		// TODO Auto-generated method stub
		for (int i = 0; i < Para.relationList_optimal.size(); i++) {

			Mig_Info mig_info = new Mig_Info();
			// *********写入该节点位置
			mig_info.setI_new_from(Para.relationList_optimal.get(i).getI_new());
			mig_info.setJ_new_from(Para.relationList_optimal.get(i).getJ_new());
			// *********写入对应的源节点位置
			mig_info.setI_old(Para.relationList_optimal.get(i).getI_old());
			mig_info.setJ_old(Para.relationList_optimal.get(i).getJ_old());
			// *********写入该节点需要扔掉哪些
			// 1.丢R
			Vector<Begin_End> r_discard_be_List = new Vector<Begin_End>();
			double r_had_begin = Para.relationList_optimal.get(i).getR_had_new().getBegin();
			double r_had_end = Para.relationList_optimal.get(i).getR_had_new().getEnd();
				Begin_End r_be = new Begin_End();
				r_be.setBegin(r_had_begin);
				r_be.setEnd(r_had_end);
				r_discard_be_List.add(r_be);
			
			for(int r_should=0;r_should<Para.relationList_optimal.get(i).getR_should_new_list().size();r_should++){
				double r_should_begin = Para.relationList_optimal.get(i).getR_should_new_list().get(r_should).getBegin();
				double r_should_end = Para.relationList_optimal.get(i).getR_should_new_list().get(r_should).getEnd();
	
			get_geometry_operation(r_should_begin,r_should_end, r_discard_be_List);
			}
			mig_info.setR_discard_list(r_discard_be_List);

			// 2.丢S
			Vector<Begin_End> s_discard_be_List = new Vector<Begin_End>();
			double s_had_begin = Para.relationList_optimal.get(i).getS_had_new().getBegin();
			double s_had_end = Para.relationList_optimal.get(i).getS_had_new().getEnd();
				Begin_End s_be = new Begin_End();
				s_be.setBegin(s_had_begin);
				s_be.setEnd(s_had_end);
				s_discard_be_List.add(s_be);
			
			for(int s_should=0;s_should<Para.relationList_optimal.get(i).getS_should_new_list().size();s_should++){
				double s_should_begin = Para.relationList_optimal.get(i).getS_should_new_list().get(s_should).getBegin();
				double s_should_end = Para.relationList_optimal.get(i).getS_should_new_list().get(s_should).getEnd();
				get_geometry_operation(s_should_begin,s_should_end, s_discard_be_List);
			}
			mig_info.setS_discard_list(s_discard_be_List);
			// 为了不报错，初始化发送list

			Vector<To_I_J_B_E> r_toijbelist = new Vector<To_I_J_B_E>();
			Vector<To_I_J_B_E> s_toijbelist = new Vector<To_I_J_B_E>();
			mig_info.setR_i_j_list(r_toijbelist);
			mig_info.setS_i_j_list(s_toijbelist);

			Para.migrationList.add(mig_info);
		}
	}

	private static void preprocessing_for_map_node_list() {
		// TODO Auto-generated method stub
		for (int i = 0; i < Para.relationList_optimal.size(); i++) {
			int row_old = Para.relationList_optimal.get(i).getI_old();
			int colume_old = Para.relationList_optimal.get(i).getJ_old();
			
			int row_new = Para.relationList_optimal.get(i).getI_new();
			int colume_new = Para.relationList_optimal.get(i).getJ_new();
			
			if(row_old!=65535 && colume_old!=65535){Para.oldnodes_optimal[row_old][colume_old].setPosition_in_list(i);}//若相等了就没有那个节点
			if(row_new!=65535 && colume_new!=65535){Para.newnodes_optimal[row_new][colume_new].setPosition_in_list(i);}
		}
	}

	private static void get_geometry_operation(double should_begin,double should_end, Vector<Begin_End> discard_List) {
		// TODO Auto-generated method stub
		for(int i=0;i<discard_List.size();i++){
			double had_begin = discard_List.get(i).getBegin();
			double had_end = discard_List.get(i).getEnd();
			if (should_begin >= had_end || should_end <= had_begin) {// 关系1: 不相交 -->q全部丢弃
				//do nothing
			} else if (should_begin >= had_begin && should_end <= had_end) {// 关系2: had全包含should--》丢弃两端不需要的
				discard_List.remove(i);
				i--;
				
				Begin_End be1 = new Begin_End();
				be1.setBegin(had_begin);
				be1.setEnd(should_begin);
				discard_List.add(be1);
	
				Begin_End be2 = new Begin_End();
				be2.setBegin(should_end);
				be2.setEnd(had_end);
				discard_List.add(be2);
			} else if (should_begin <= had_begin && should_end >= had_end) {// 关系3: should全包含had--》全不丢弃
				discard_List.remove(i);
				i--;
			} else {// 关系4: 部分的包含--》丢弃一端
				Begin_End r_be = new Begin_End();
				if (should_begin >= had_begin && should_end >= had_end) {
					r_be.setBegin(had_begin);
					r_be.setEnd(should_begin);
				} else {
					r_be.setBegin(should_end);
					r_be.setEnd(had_end);
				}
				discard_List.remove(i);
				i--;
				discard_List.add(r_be);
			}
		}
	}

	private static void preprocessing_for_migration_inti_needlist() {
		// TODO Auto-generated method stub
		for (int i = 0; i < Para.relationList_optimal.size(); i++) {

			// preprocessing for r
			double r_had_begin = Para.relationList_optimal.get(i).getR_had_new().getBegin();
			double r_had_end = Para.relationList_optimal.get(i).getR_had_new().getEnd();

			Vector<Begin_End> r_nd_list = new Vector<Begin_End>();
			Vector<Begin_End> s_nd_list = new Vector<Begin_End>();
			Para.relationList_optimal.get(i).setR_need_list(r_nd_list);
			Para.relationList_optimal.get(i).setS_need_list(s_nd_list);

			for(int r_should=0;r_should<Para.relationList_optimal.get(i).getR_should_new_list().size();r_should++){
				double r_sould_begin = Para.relationList_optimal.get(i).getR_should_new_list().get(r_should).getBegin();
				double r_sould_end = Para.relationList_optimal.get(i).getR_should_new_list().get(r_should).getEnd();
				if (r_sould_begin >= r_had_end || r_sould_end <= r_had_begin) {// 关系1:// 不相交
				Begin_End r_be = new Begin_End();
				r_be.setBegin(r_sould_begin);
				r_be.setEnd(r_sould_end);
				Para.relationList_optimal.get(i).getR_need_list().add(r_be);
			} else if (r_sould_begin >= r_had_begin && r_sould_end <= r_had_end) {// 关系2:had全包含should
				Begin_End r_be = new Begin_End();
				r_be.setBegin(0);
				r_be.setEnd(0);
				Para.relationList_optimal.get(i).getR_need_list().add(r_be);
			} else if (r_sould_begin <= r_had_begin && r_sould_end >= r_had_end) {// 关系3:should全包含had
				Begin_End r_be_1 = new Begin_End();
				r_be_1.setBegin(r_sould_begin);
				r_be_1.setEnd(r_had_begin);
				Begin_End r_be_2 = new Begin_End();
				r_be_2.setBegin(r_had_end);
				r_be_2.setEnd(r_sould_end);
				Para.relationList_optimal.get(i).getR_need_list().add(r_be_1);
				Para.relationList_optimal.get(i).getR_need_list().add(r_be_2);
			} else {// 关系4: 部分的包含
				Begin_End r_be = new Begin_End();
				if (r_sould_begin >= r_had_begin && r_sould_end >= r_had_end) {
					r_be.setBegin(r_had_end);
					r_be.setEnd(r_sould_end);
				} else {
					r_be.setBegin(r_sould_begin);
					r_be.setEnd(r_had_begin);
				}
				Para.relationList_optimal.get(i).getR_need_list().add(r_be);
			}}
			
			// preprocessing for s
			double s_had_begin = Para.relationList_optimal.get(i).getS_had_new().getBegin();
			double s_had_end = Para.relationList_optimal.get(i).getS_had_new().getEnd();

			for(int s_should=0;s_should<Para.relationList_optimal.get(i).getS_should_new_list().size();s_should++){
				double s_sould_begin = Para.relationList_optimal.get(i).getS_should_new_list().get(s_should).getBegin();
				double s_sould_end = Para.relationList_optimal.get(i).getS_should_new_list().get(s_should).getEnd();
				if (s_sould_begin >= s_had_end || s_sould_end <= s_had_begin) {// 关系1:不相交
				Begin_End s_be = new Begin_End();
				s_be.setBegin(s_sould_begin);
				s_be.setEnd(s_sould_end);
				Para.relationList_optimal.get(i).getS_need_list().add(s_be);
			} else if (s_sould_begin >= s_had_begin && s_sould_end <= s_had_end) {// 关系2:had全包含should
				Begin_End s_be = new Begin_End();
				s_be.setBegin(0);
				s_be.setEnd(0);
				Para.relationList_optimal.get(i).getS_need_list().add(s_be);
			} else if (s_sould_begin <= s_had_begin && s_sould_end >= s_had_end) {// 关系3:should全包含had
				Begin_End s_be_1 = new Begin_End();
				s_be_1.setBegin(s_sould_begin);
				s_be_1.setEnd(s_had_begin);
				Begin_End s_be_2 = new Begin_End();
				s_be_2.setBegin(s_had_end);
				s_be_2.setEnd(s_sould_end);
				Para.relationList_optimal.get(i).getS_need_list().add(s_be_1);
				Para.relationList_optimal.get(i).getS_need_list().add(s_be_2);
			} else {// 关系4: 部分的包含
				Begin_End s_be = new Begin_End();
				if (s_sould_begin >= s_had_begin && s_sould_end >= s_had_end) {
					s_be.setBegin(s_had_end);
					s_be.setEnd(s_sould_end);
				} else {
					s_be.setBegin(s_sould_begin);
					s_be.setEnd(s_had_begin);
				}
				Para.relationList_optimal.get(i).getS_need_list().add(s_be);
			}
			}
		}
	}

}
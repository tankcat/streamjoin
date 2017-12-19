package bolt.mfm.migrationplan;

import java.util.Vector;



public class Do_optimal {
	public static void initialize_nodes(old_Node_optimal[][] oldnodes, double oldR, double oldS, int mr_row, int ns_col, int last_num, String lack_r_or_s, int v) {
		// TODO Auto-generated method stub
		
		double r_v; double s_v;
		
		//System.out.println(lack_r_or_s+": "+last_num);
		
		switch (lack_r_or_s) {
		case "col":// 行    为 primary 在列上存  最后
			r_v = oldR/mr_row; s_v = v - r_v;
			
			for(int i=0;i<mr_row;i++){
				for(int j=0;j<ns_col;j++){
					oldnodes[i][j] = new old_Node_optimal();
					oldnodes[i][j].setActivity(false);
					if(j==ns_col-1){//不正常的行和列
						if(last_num ==0){continue;}
						last_num--;
					}
					oldnodes[i][j].setActivity(true);
					
					oldnodes[i][j].setR_begin((double)(i)/mr_row);
					oldnodes[i][j].setR_end((double)(i+1)/mr_row);
					
					oldnodes[i][j].setS_begin((double)(j)/ns_col);
					oldnodes[i][j].setS_end((double)(j+1)/ns_col);
				}
			}
			break;
		case "row":// 列    为 primary  在行上存  最后
			s_v = oldS/ns_col; r_v = v-s_v;
			
			//System.out.println("row: " +" mr_row: "+mr_row+" ns_col: "+ns_col+ " r_v: "+r_v +" s_v "+s_v+" last_s_v "+last_s_v+" last_r_v "+last_r_v+" last_num: "+last_num);
			
			for(int i=0;i<mr_row;i++){
				for(int j=0;j<ns_col;j++){
					oldnodes[i][j] = new old_Node_optimal();
					oldnodes[i][j].setActivity(false);
					//System.out.println("jinlaila"+ i +" : "+(mr_row-1));
					if(i == (mr_row-1)){//不正常的行和列
						if(last_num ==0){continue;}
						last_num--;
					}
					oldnodes[i][j].setActivity(true);//正常的行和列
	
					oldnodes[i][j].setR_begin((double)(i)/mr_row);
					oldnodes[i][j].setR_end((double)(i+1)/mr_row);
					
					oldnodes[i][j].setS_begin((double)(j)/ns_col);
					oldnodes[i][j].setS_end((double)(j+1)/ns_col);
				}
			}
			break;
		default:
			break;
		}
	}
	public static void get_row_col(double oldR, double oldS, int v) {
		// TODO Auto-generated method stub
		String[] costcase = {"ru","rl","su","sl"};
		
		double mincost=-1;
		for(int i=0;i<costcase.length;i++){
			mincost = get_all_cost(oldR,oldS,v,costcase[i],mincost);
		}
//		System.out.println("mincost: "+mincost+" mr: "+Para.mr_row+" ns: "+Para.ns_col+" r_or_s: "+Para.lack_row_or_col+" lack_num: "+Para.last_num);
	}

	private static double get_all_cost(double oldR, double oldS, int v, String string, double mincost) {
		// TODO Auto-generated method stub
		double halfv = (double)v/2;
		double primary = 0;
		double aid = 0;
		int num = 0;
		
		switch (string) {
		case "ru":
			primary = oldR; aid = oldS; num = (int) Math.ceil(oldR/halfv);
			break;
		case "rl":
			primary = oldR; aid = oldS; num = (int) Math.floor(oldR/halfv);
			break;
		case "su":
			primary = oldS; aid = oldR; num = (int) Math.ceil(oldS/halfv);
			break;
		case "sl":
			primary = oldS; aid = oldR; num = (int) Math.floor(oldS/halfv);
			break;
		default:
			break;
		}
		if(num ==0 ){num=1;}
		
		double p_v = primary/num;
		double remaind_v = v - p_v;
		
		int another_num = (int) Math.floor(aid/remaind_v);//0?
		double aid_surplus = aid - another_num*remaind_v;
		
		double last_node_surplus =0;
		int the_lack_num = 0;
		if(aid_surplus !=0){
			 last_node_surplus = v - aid_surplus;
		     the_lack_num = (int) Math.ceil(primary/last_node_surplus);
		     another_num +=1;
		}
	
		double thecost = primary*(another_num)+(aid-aid_surplus)*(num)+aid_surplus*the_lack_num;
		
		//ln(thecost+"=========================================");
		
		int node_num = 0 ;
		if(aid_surplus !=0){node_num = num*(another_num-1)+the_lack_num;}
		else{node_num = num*another_num;}
		
		//.println(string+" --> primary :"+primary+" num: "+num+" aid: "+aid+" another_num: "+another_num
				//+" the_lack_num: "+the_lack_num+" node_num: " + node_num);

		if(mincost == -1 || mincost>thecost){
			mincost = thecost;
			
			switch (string) {
			case "ru":
				Para.mr_row = num;
				Para.ns_col = another_num;
				Para.lack_row_or_col = "col";
				break;
			case "rl":
				Para.mr_row = num;
				Para.ns_col = another_num;
				Para.lack_row_or_col = "col";
				break;
			case "su":
				Para.mr_row = another_num;
				Para.ns_col = num;
				Para.lack_row_or_col = "row";
				break;
			case "sl":
				Para.mr_row = another_num;
				Para.ns_col = num;
				Para.lack_row_or_col = "row";
				break;
			default:
				break;
			}
			if(the_lack_num==0){
				Para.last_num = num;
				}
			else {
				Para.last_num = the_lack_num;
			}
		}
		return mincost;
	}
	public static void initialize_new_nodes(
			new_Node_optimal[][] newnodes_optimal, double newR, double newS,
			int new_row_num, int new_col_num, int last_num, String lack_row_or_col, int v) {
		// TODO Auto-generated method stub
		for(int i=0;i<new_row_num;i++){//初始化节点
			for(int j=0;j<new_col_num;j++){ 
				newnodes_optimal[i][j] = new new_Node_optimal();}}
		
        double r_range; double s_range;
        int old_row_num = Para.oldnodes_optimal.length;
        int old_col_num = Para.oldnodes_optimal[0].length;
		//System.out.println(lack_row_or_col+": "+last_num);
			r_range = (double)(1)/new_row_num; s_range = (double)(1)/new_col_num;
			//=================================================写入R
			if(new_row_num>old_row_num){//如果新节点的行数多于旧节点
				Vector<Begin_End> r_list_remain = new Vector<Begin_End>();
				int old_r_num = old_row_num;
				for(int i=0;i<new_row_num;i++){
					// System.out.println("old_r_num: "+old_r_num);
						 if(old_r_num>0){//起点等于旧节点
							old_r_num--;
							Begin_End be1 = new Begin_End();
							double thisbegin = Tools_optimal.get_double(Para.oldnodes_optimal[i][0].getR_begin());
							double thisend = Tools_optimal.get_double(Para.oldnodes_optimal[i][0].getR_end());
							double thismidpoint = thisbegin+r_range;
							be1.setBegin(thismidpoint);
							be1.setEnd(thisend);
							
							for(int j=0;j<new_col_num;j++){	
								Vector<Begin_End> r_list = new Vector<Begin_End>();
								Begin_End be = new Begin_End();
								be.setBegin(thisbegin);
								be.setEnd(thismidpoint);
								r_list.add(be);
								newnodes_optimal[i][j].setActivity(true);
								newnodes_optimal[i][j].setR_list(r_list);
							}
							r_list_remain.add(be1);
							}
						else{//琐碎的聚合进来
							//System.out.println("进来处理琐碎的了！！！");
							Vector<Begin_End> r_list_this_put = new Vector<Begin_End>();
							double countrange=0;							
							for(int lnum=0;lnum<r_list_remain.size();lnum++){
								Begin_End sub_be_remain = null;
								if(countrange<r_range){
									Begin_End be_put = new Begin_End();
									double begin = r_list_remain.get(0).getBegin();
									double end = r_list_remain.get(0).getEnd();
									double sub_range = end - begin;
									if((countrange+sub_range)<=r_range){//直接放入
										be_put.setBegin(begin);
										be_put.setEnd(end);									
										countrange+=sub_range;
										countrange=countrange;
									}else{//拆开
										sub_be_remain = new Begin_End();
										double sub_sub_range = r_range-countrange;//还要用多少
										be_put.setBegin(begin);
										be_put.setEnd(begin+sub_sub_range);
										/*System.out.println(countrange+" : "+r_range+" : "+sub_sub_range+" :  "+be_put.getBegin()+"-"+be_put.getEnd()+"   "+
												sub_be_remain.getBegin()+"-"+sub_be_remain.getEnd());*/
										sub_be_remain.setBegin(begin+sub_sub_range);
										sub_be_remain.setEnd(end);
										countrange+=sub_sub_range;
										/*System.out.println(countrange+" : "+r_range+" : "+sub_sub_range+" :  "+be_put.getBegin()+"-"+be_put.getEnd()+"   "+
												sub_be_remain.getBegin()+"-"+sub_be_remain.getEnd());*/
									}
									r_list_this_put.add(be_put);
									r_list_remain.remove(0);
									lnum--;
									if(sub_be_remain!=null){r_list_remain.add(sub_be_remain);}
								}
							}
							for(int j=0;j<new_col_num;j++){
								newnodes_optimal[i][j].setActivity(true);
								newnodes_optimal[i][j].setR_list(r_list_this_put);
							}
							}}	
			}
			else{//如果新节点的行少于旧节点
				int new_r_num = new_row_num;
				for(int i=0;i<old_row_num;i++){
					 //System.out.println("old_r_num: "+new_r_num);
				 if(new_r_num>0){//起点等于旧节点
					 new_r_num--;
					Begin_End be1 = new Begin_End();
					for(int j=0;j<new_col_num;j++){	//新节点中这一行的每一个节点 先将就值赋值
						Vector<Begin_End> r_list = new Vector<Begin_End>();
						Begin_End be = new Begin_End();
						newnodes_optimal[i][j].setActivity(true);
						double old_begin = Para.oldnodes_optimal[i][0].getR_begin();
						double old_end = Para.oldnodes_optimal[i][0].getR_end();
						be.setBegin(old_begin);
						be.setEnd(old_end);
						
						r_list.add(be);
						newnodes_optimal[i][j].setR_list(r_list);
					}
					}
					else{//琐碎的聚合进来-->把old剩下节点分到新节点
						//System.out.println("进来处理琐碎的了！！！");
						
						double this_old_begin_r = Para.oldnodes_optimal[i][0].getR_begin();
						double this_old_end_r = Para.oldnodes_optimal[i][0].getR_end();
						double new_shold_r = (double)(1)/new_row_num;
						
						//System.out.println("this begin and end: "+this_old_begin_r+"-"+this_old_end_r);
						boolean mark = true;
						 
							for(int row_num=0;row_num<new_row_num;row_num++){
								if(mark){
									double old_r_had = this_old_end_r - this_old_begin_r;
								
								//System.out.println(row_num+"-"+this_old_begin_r+"-"+this_old_end_r);
								//计算该行的节点已有多少R
								double new_r_had = 0;
								for(int rr=0;rr<newnodes_optimal[row_num][0].getR_list().size();rr++){
									double thisbegin = newnodes_optimal[row_num][0].getR_list().get(rr).getBegin();
									double thisend = newnodes_optimal[row_num][0].getR_list().get(rr).getEnd();
									new_r_had+=(thisend-thisbegin);}
								
								//System.out.println("======="+row_num+"new_r_had-"+new_r_had);
								if(new_r_had<new_shold_r){
									Begin_End be = new Begin_End();
									if((new_r_had+old_r_had)<new_shold_r){
										be.setBegin(this_old_begin_r);
										be.setEnd(this_old_end_r);
										mark=false;
									}else{
										double mid_point = new_shold_r - new_r_had;
										be.setBegin(this_old_begin_r);
										be.setEnd(this_old_begin_r+mid_point);
										this_old_begin_r = this_old_begin_r+mid_point;
									}
									for(int j=0;j<new_col_num;j++){
										newnodes_optimal[row_num][j].setActivity(true);
										newnodes_optimal[row_num][j].getR_list().add(be);
									}
								}
							}}
						}}}

//			for(int rr=0;rr<new_row_num;rr++){
//				for(int cc=0;cc<new_col_num;cc++){
//					for(int rl=0;rl<newnodes_optimal[rr][cc].getR_list().size();rl++){
//						System.out.println("["+rr+""+cc+"]  R: ["+newnodes_optimal[rr][cc].getR_list().get(rl).getBegin()+"-"+
//								newnodes_optimal[rr][cc].getR_list().get(rl).getEnd()+"]");
//					}
//				}
//			}
			
			
			//写入S
			if(new_col_num>old_col_num){//新节点的列数多于就节点
				Vector<Begin_End> s_list_remain = new Vector<Begin_End>();
				int old_s_num = old_col_num;
				for(int j=0;j<new_col_num;j++){
				  if(old_s_num>0){//新节点  旧起点
					old_s_num--;
					Begin_End be1 = new Begin_End();
					for(int i=0;i<new_row_num;i++){
						Vector<Begin_End> s_list = new Vector<Begin_End>();
						Begin_End be = new Begin_End();

						newnodes_optimal[i][j].setActivity(true);
						
						double thisbegin = (double)(j)/old_col_num;
						be.setBegin(thisbegin);
						be.setEnd(thisbegin+s_range);
						be1.setBegin(thisbegin+s_range);
						be1.setEnd((double)(j+1)/old_col_num);;
						
						
						s_list.add(be);
						newnodes_optimal[i][j].setS_list(s_list);
					}
					s_list_remain.add(be1);
					}
					else{//琐碎的聚合进来
						Vector<Begin_End> s_list_this_put = new Vector<Begin_End>();
						double countrange=0;
						Begin_End sub_be_remain = null;
						for(int lnum=0;lnum<s_list_remain.size();lnum++){
							if(countrange<s_range){
								Begin_End be_put = new Begin_End();
								double begin = s_list_remain.get(0).getBegin();
								double end = s_list_remain.get(0).getEnd();
								double sub_range = end - begin;
								if((countrange+sub_range)<=s_range){
									be_put.setBegin(begin);
									be_put.setEnd(end);									
									countrange+=sub_range;
								}else{
									sub_be_remain = new Begin_End();
									double sub_sub_range = s_range-countrange;
									be_put.setBegin(begin);
									be_put.setEnd(begin+sub_sub_range);
									
									sub_be_remain.setBegin(begin+sub_sub_range);
									sub_be_remain.setEnd(end);
									countrange+=sub_sub_range;
								}
								s_list_this_put.add(be_put);
								s_list_remain.remove(0);
								lnum--;
								if(sub_be_remain!=null){s_list_remain.add(sub_be_remain);}
							}
						}
						for(int i=0;i<new_row_num;i++){//赋值的一列的每一行上
							newnodes_optimal[i][j].setActivity(true);
							newnodes_optimal[i][j].setS_list(s_list_this_put);
						}}}}
			else{//如果新节点的列数少于就节点
				int new_s_num = new_col_num;
				for(int j=0;j<old_col_num;j++){
					 //System.out.println("old_r_num: "+new_s_num);
				 if(new_s_num>0){//起点等于旧节点
					 new_s_num--;
					Begin_End be1 = new Begin_End();
					for(int i=0;i<new_row_num;i++){	//新节点中这一列的每一个节点 先将就值赋值
						Vector<Begin_End> s_list = new Vector<Begin_End>();
						Begin_End be = new Begin_End();
						newnodes_optimal[i][j].setActivity(true);
						double old_begin = Para.oldnodes_optimal[0][j].getS_begin();
						double old_end = Para.oldnodes_optimal[0][j].getS_end();
						be.setBegin(old_begin);
						be.setEnd(old_end);
						
						s_list.add(be);
						newnodes_optimal[i][j].setS_list(s_list);
					}
					}
					else{//琐碎的聚合进来-->把old剩下节点分到新节点
						//System.out.println("进来处理琐碎的了！！！");
						
						double this_old_begin_s = Para.oldnodes_optimal[0][j].getS_begin();
						double this_old_end_s = Para.oldnodes_optimal[0][j].getS_end();
						double new_shold_s = Tools_optimal.get_double((double)(1)/new_col_num);
						boolean mark = true;
						
						for(int col_num=0;col_num<new_col_num;col_num++){
							if(mark){
							double old_s_had = this_old_end_s - this_old_begin_s;
							//计算该节点已有多少R
							double new_s_had = 0;
							
							for(int ss=0;ss<newnodes_optimal[0][col_num].getS_list().size();ss++){
								double thisbegin = newnodes_optimal[0][col_num].getS_list().get(ss).getBegin();
								double thisend = newnodes_optimal[0][col_num].getS_list().get(ss).getEnd();
								new_s_had+=(thisend-thisbegin);}
							
							if(new_s_had<new_shold_s){
								Begin_End be = new Begin_End();
								if((new_s_had+old_s_had)<new_shold_s){
									be.setBegin(this_old_begin_s);
									be.setEnd(this_old_end_s);
									mark = false;
								}else{
									double mid_point = new_shold_s - new_s_had;
									be.setBegin(this_old_begin_s);
									be.setEnd(this_old_begin_s+mid_point);
									this_old_begin_s = this_old_begin_s+mid_point;
								}
								for(int i=0;i<new_row_num;i++){
									newnodes_optimal[i][col_num].setActivity(true);
									newnodes_optimal[i][col_num].getS_list().add(be);
								}
							}}
						}
						}}
			}


			if(lack_row_or_col.equals("col")){// 行    为 primary 在列上存  最后
				int the_last_col = new_col_num-1;
				for(int rr=last_num;rr<new_row_num;rr++){
					newnodes_optimal[rr][the_last_col].setActivity(false);
				}
			}
			if(lack_row_or_col.equals("row")){
				int the_last_row = new_row_num-1;
				for(int cc=last_num;cc<new_col_num;cc++){
					newnodes_optimal[the_last_row][cc].setActivity(false);
				}
			}
		}
	}

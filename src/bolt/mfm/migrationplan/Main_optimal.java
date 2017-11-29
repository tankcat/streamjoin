package bolt.mfm.migrationplan;



public class Main_optimal {
	public static void main(String[] args) {
		
		Do_optimal.get_row_col(Para.oldR,Para.oldS,Para.V);
		Para.oldnodes_optimal = new old_Node_optimal[Para.mr_row][Para.ns_col];
		Do_optimal.initialize_nodes(Para.oldnodes_optimal,Para.oldR,Para.oldS,Para.mr_row,Para.ns_col,Para.last_num,Para.lack_row_or_col, Para.V);
		
		System.out.println("this is old: ");
		Tools_optimal.printnodes(Para.oldnodes_optimal);
		
		Para.oldR = Para.newR; Para.oldS = Para.newS;
		
		Do_optimal.get_row_col(Para.newR,Para.newS,Para.V);
		Para.newnodes_optimal = new new_Node_optimal[Para.mr_row][Para.ns_col];
		Do_optimal.initialize_new_nodes(Para.newnodes_optimal,Para.newR,Para.newS,Para.mr_row,Para.ns_col,Para.last_num,Para.lack_row_or_col, Para.V);
		
		System.out.println("this is new: ");
		Tools_optimal.printnodes(Para.newnodes_optimal);
		
		Do2_optimal.make_map(Para.oldnodes_optimal,Para.newnodes_optimal);
		
		Do3_optimal.gen_migration_plan(Para.relationList_optimal);
		
		Do4.Merge_migration_plan(Para.migrationList);
		
		Tools_optimal.print_migration_plan(Para.migrationList,0);
		
		System.out.println(Para.mr_row+" , "+Para.ns_col);
	}
}

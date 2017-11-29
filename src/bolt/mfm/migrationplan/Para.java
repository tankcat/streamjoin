package bolt.mfm.migrationplan;

import java.util.Vector;

import bolt.mfm.migrationplan.Old_New_Relation_optimal;
import bolt.mfm.migrationplan.new_Node_optimal;
import bolt.mfm.migrationplan.old_Node_optimal;

public class Para {
	public static int V=80000;
	
	public static double oldR = 56495;
	public static double oldS = 55944;
	public static Node oldnodes[][];
	public static old_Node_optimal oldnodes_optimal[][];
	
	
	public static int mr_row = 0;
	public static int ns_col = 0;
	public static String lack_row_or_col="";
	public static int last_num = 0;
		
	public static double newR = 115020;
	public static double newS = 114747;
	public static Node newnodes[][];
	public static new_Node_optimal newnodes_optimal[][];
	
	
	
	public static Vector<Mig_Info> migrationList =new Vector<Mig_Info>();
	
	public static Vector<Old_New_Relation_optimal> relationList_optimal =new Vector<Old_New_Relation_optimal>();
}

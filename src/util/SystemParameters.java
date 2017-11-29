package util;

public class SystemParameters {
	//信号量
	public static final String ORI_DATA="original-data";
	public static final String TICK="tick-tuple";
	public static final String CHANGE="scheme-change";
	public static final String JOINER_MIG_END="joiner-migrate-end";
	public static final String CTRL_MIG_END="controller-migrate-end";
	public static final String MIG_DATA="migrated-data";
	public static final String JOINER_MIG_EOF="migrate-eof";
	public static final String STORE="tuple-for-store-and-join";
	public static final String JOIN="tuple-for-join";
	public static final String GOON="begin-report";

	
	//tuple字段
	public static final String KEY="primary-key";
	public static final String CONTENT="record-content";
	public static final String SIGNAL="tuple-signal";
	public static final String FIRST="first";
	public static final String SPECIAL="special";
	public static final String FIELD_1="field1";
	public static final String FIELD_2="field2";
	public static final String FIELD_3="field3";
	public static final String ROUTE="route-by-table";
	
	//redis相关
	public static final String HOST_53="10.11.1.53";
	public static final String HOST_56="10.11.1.56";
	public static final String HOST_LOCAL="127.0.0.1";
	public static final int PORT=6379;
	
	//topology相关
	public static final String MFMSPOUT_ORDER="mfm-spout-order";
	public static final String MFMSPOUT_LI="mfm-spout-lineitem";
	public static final String MFMROUTER="mfm-router";
	public static final String MFMJOINER="mfm-joiner";
	public static final String MFMCONTROLLER="mfm-controller";
	public static final String MFMTOPO="mfm-topology";
	public static final String LBTOPO="loadbalance-topology";
	public static final String LBSPOUT="loadbalance-spout";
	public static  final String UBOLT="ubolt";
	public static final String DBOLT="dbolt";
	public static final String LBCONTROLLER="load-balance-controller";
	//并行度
	public static final int SPOUT_PARA=4;
	public static final int ROUTER_PARA=4;
	public static final int JOINER_PARA=100;
	public static final int CTRL_PARA=1;
	public static final int UDBOLT_PARA=5;
	public static final int DBOLT_PARA=5;
	public static final int DBOLT_PARA_INIT=5;
	
	public static final String INITIAL_MATRIX="1-1";
	public static final int EMIT_FREQUENCY=5;
	public static final int USD_JOINER_NUM=1;
	
	public static final long SLEEP=600000;
	public static final int WORKER_NUM=30;
	public static final int SPOUT_MAX_PEND=1000;
	public static final double theta=0.01;
}

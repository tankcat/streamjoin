package tool;

public class SystemParameters {
	
	public static int emitFrequencyInSeconds=25;
	public static String host="127.0.0.1";
	public static int port=6379;
	public static String dataSource="rawdata";
	public static final String routingTable="route-table";
	public static final int range =10;
	public static final String report="report";
	
	public static final double thetaBalance=0.1;
	
	
	public static final String keySpoutId="key-spout";
	
	public static final String uBoltId="up-bolt";
	public static final String dBoltId="down-bolt";
	public static final String ctrlBoltId="controller-bolt";
	public static final String ldTopologyId="load-balance-topology";
	
	public static final int keySpoutPara=10;
	
	public static final int uBoltPara=8;
	public static final int dBoltPara=30;
	public static final int ctrlBoltPara=1;
	
	//Tuple fields value 
	public static final String updateTable="update-routing-key";
	public static final String completeMigrate="complete-migration-key";
	public static final String migrateSignal="migrate-signal-to-dbolt";
	public static final String migratedKeys="migrated-keys";
	public static final String keyLoadInfo="keys-load-info";
	public static final String join="tuple-for-join";
	public static final String store="tuple-for-store";
	public static final String sendEnd="send-end";

	
	//tuple fields name
	public static final String weiBoTime="weibo-time";
	public static final String topicWord="topic-word";
	public static final String topicInfo="keys-info";
	public static final String taskIndex="task-index";
	public static final String text="text";
	public static final String key="key";
	public static final String sendTime="send-time";
	public static final String signal="signal";
	public static final String fieldOption1="field1";
	public static final String fieldOption2="field2";
	public static final String fieldOption3="field3";
	public static final String relation="relation";
	public static final String tuple="normal-tuple";
	public static final String migratePlan="migrate-plans";
}

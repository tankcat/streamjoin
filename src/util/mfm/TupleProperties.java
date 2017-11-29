package util.mfm;

public interface TupleProperties {

	public static final int DEFAULT_NUM_ACKERS = 0;

	//Tuple �ֶ�
	public static final String COPM_INDEX="ComponentIndex";
	public static final String TUPLE="Tuple";
	public static final String TIMESTAMP="Timestamp";
	public static final String EPOCH="Epoch";

	public static final String DIMENSION="Dimension";
	public static final String RESHU_SIGNAL="ReshufflerSignal";
	public static final String MAPPING="Mapping";
	public static final String HASH="Hash";
	public static final String NORMAL="Normal";//�Ӵ�ֱ����ˮƽ����Ϊ1����б�Խ�Ϊ0
	public static final String NEWNODE="Newnode";//���͸�����ӵĽ��Ϊ1������Ϊ0
	
	public static final String MESSAGE="Message";
	public static final String MIGRATE_ROUTE="Migrate-route";
	public static final String CTRL_SIGNAL="controllerSignal";
	
	public static final String PLAN="plan";
	public static final String TICKTIME="ticktime";
	
	public static final String DELNUM="deletenumber";
	public static final String SPECIAL="special";
	public static final String REGULAR="regular";
	
	public static final String StoreOrJoin="store_join";
	
	public static final String KEYVALUE="key_value";
	public static final String KEYLOADPERNODE="key_load_per_node";
	public static final String TASKINDEX="task_index";
	public static final String KEYMIGRATIONITEM="key_migration_item";
	public static final String MIGRATINGKEY="migrating_key";
	public static final String KEYHASHCODE="key_hash_code";
}

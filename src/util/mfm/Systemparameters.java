package util.mfm;

import java.util.Map;

import org.apache.storm.utils.Utils;

public class Systemparameters {
	public static final String DATA_STREAM=Utils.DEFAULT_STREAM_ID;
	
	public static final String LAST_ACK="LAST_ACK";

	//Theta join stream IDs
	//
	public static final String ThetaDataMigrationJoinerToReshuffler="2";
	//
	public static final String ThetaDataMigrationReshufflerToJoiner="3";
	//
	public static final String ThetaDataReshufflerToJoiner="4";
	//
	public static final String ThetaJoinerAcks="5";
	//
	public static final String ThetaReshufflerStatus="6";
	//
	public static final String ThetaSynchronizerSignal="7";
	//
	public static final String ThetaReshufflerSignal="8";

	
	
	

	//
	public static final String ThetaSignalStop="Stop";

	//
	public static final String ThetaSignalDataMigrationEnded="D-M-E";
	public static final String ThetaJoinerDataMigrationEOF="D-M-E-E-O-F";
	public static final String ThetaJoinerMigrationSignal="T-J-M-S";
	public static final String ThetaAckDataMigrationEnded="ACK-1";

	
	public static long MemoryV=200;
	
	//lowLoad,V,overLoad,up
	public static double[] loadBoundary={0.2,0.5,0.8,0.6};
	
	//
	public static final String ThetaJoinerSignal="9";
	public static final String ThetaDataMigration="10";
	public static final String ThetaControllerSignal="11";

	
	public static final String ThetaDataSendEOF="14";
	
	//
	public static final String DataMigrationEOF="joiner-migration-eof";
	public static final String DataMigrationEnded="joiner-migration-ended";
	public static final String DtatMigrationFinalize="joiner-migration-finalize";
	


	
	public static final String KeyBasedDataStream="key_based_data";
	public static final String ReportWorkloadSignal="report_workload_signal";
	public static final String MigrationSignal="key_migrate_signal";
	public static final String MigrationKey="key_migrate";
	public static final String MigrationEnded="migrate_end";
	
	public static int getInt(Map conf,String key){
		final String value=(String) conf.get(key);
		return Integer.parseInt(value);
	}

}

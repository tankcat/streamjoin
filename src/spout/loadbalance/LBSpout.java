package spout.loadbalance;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import util.SystemParameters;

import java.util.List;
import java.util.Map;

/**
 * Created by stream on 16-9-18.
 */
public class LBSpout extends BaseRichSpout{
    private transient Jedis jedis;
    private SpoutOutputCollector _collector;
    private String dataSource;

    public LBSpout(String dataSource){
        this.dataSource=dataSource;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        jedis=getConnectJedis();
        _collector=spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        if(jedis.exists(dataSource)){
            String line=jedis.lpop(dataSource);
            if(line!=null&&!line.equals("")){
                //System.out.println(line);
                boolean isFirst=(Math.random()>0.5?true:false);
                int key=Integer.parseInt(line.split("\\|")[0]);
                Values value=new Values(SystemParameters.ORI_DATA,key,isFirst,line);
                _collector.emit(value,line);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(SystemParameters.SIGNAL,SystemParameters.KEY,SystemParameters.FIRST,SystemParameters.CONTENT));
    }

    private Jedis getConnectJedis(){
        if(jedis==null)
            jedis=new Jedis(SystemParameters.HOST_LOCAL,SystemParameters.PORT);
        return jedis;
    }

    private void getDisconnectJedis(){
        if(jedis!=null){
            jedis.disconnect();
            jedis=null;
        }
    }
}

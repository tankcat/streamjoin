package tool;

import redis.clients.jedis.Jedis;

/**
 * Created by Tankc on 2017/10/16.
 */
public class ResetRedis {

    public static void main(String[] args){
        Jedis jedis=new Jedis(SystemParameters.host,SystemParameters.port);
        jedis.del("statistics");
        jedis.set("newMap","");
        jedis.set("curEpochNumber",0+"");
        jedis.set("ticktime",0+"");
        jedis.del("usedJoiner");
        jedis.del("tick");
        jedis.del("expinfo");
        jedis.del("migTo");
        jedis.del("plan");
        jedis.del("matrix");
        jedis.set("usedJoiner","");
        jedis.del("migTime");
        jedis.del("migVolume");
        jedis.del("newSchemeInfo");
        for(int i=0;i<8;i++) {
            jedis.del("plan_items"+i);
        }
        System.out.println("Reset Redis Done!");
    }
}

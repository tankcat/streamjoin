package tool.hash;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by stream on 16-9-17.
 */
public class ConsistentHashWithoutVirtual {
    //待加入hash环的节点列表
    private List<String> servers;
    //key表示节点的hash值，value表示节点的名称
    private SortedMap<Integer,String> sortedMap=new TreeMap<>();
    public ConsistentHashWithoutVirtual(List<String> servers){
        this.servers=servers;
        for(String server:servers){
            int hash=getHash(server);
            sortedMap.put(hash,server);
        }
    }

    //使用FNV1_32_HASH算法计算节点的hash值
    private int getHash(String server){
        final int p=16777619;
        int hash=(int)2166136261L;
        for(int i=0;i<server.length();i++){
            hash=(hash^server.charAt(i))*p;
        }
        hash += hash<<13;
        hash += hash>>7;
        hash += hash<<3;
        hash ^= hash>>17;
        hash += hash<<5;

        if(hash<0)
            hash=Math.abs(hash);
        return hash;
    }

    public String getServer(String key){
        int hash=getHash(key);
        SortedMap<Integer,String> subMap=sortedMap.tailMap(hash);
        Integer i=subMap.firstKey();
        return sortedMap.get(i);
    }
}

package tool.hash;

import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by stream on 16-9-17.
 */
public class ConsistentHash {

    //真实节点列表
    private List<String> realNodes=new LinkedList<>();
    //虚拟节点，key表示节点的hash值，value表示节点的名称
    private SortedMap<Integer,String> virtualNodes=new TreeMap<>();

    private final int Virtual_nodes;

    public ConsistentHash(LinkedList<String> servers,int virtual_nodes){
        this.Virtual_nodes=virtual_nodes;
        this.realNodes=servers;
        for(String server:realNodes){
            for(int i=0;i<virtual_nodes;i++){
                String virtualNodeName=server+"&&VN"+String.valueOf(i);
                int hash=getHash(virtualNodeName);
                virtualNodes.put(hash,virtualNodeName);
            }
        }
    }

    private int getHash(String server){
        final int p=16777619;
        int hash=(int)2166136261L;
        for(int i=0;i<server.length();i++){
            hash=(hash^server.charAt(i))*p;
        }
        hash += hash<<13;
        hash ^= hash>>7;
        hash += hash<<3;
        hash ^= hash>>17;
        hash += hash<<5;
        if(hash<0)
            hash=Math.abs(hash);
        return hash;
    }

    public String getServer(String key){
        int hash=getHash(key);
        SortedMap<Integer,String> subMap=virtualNodes.tailMap(hash);
        Integer i=subMap.firstKey();
        String virtualNode=subMap.get(i);
        return virtualNode.substring(0,virtualNode.indexOf("&&"));
    }

    public void addNewNode(String node){
        realNodes.add(node);
        for(int i=0;i<Virtual_nodes;i++){
            String virtualNodeName=node+"&&VN"+String.valueOf(i);
            int hash=getHash(virtualNodeName);
            virtualNodes.put(hash,virtualNodeName);
        }
    }

    public void removeOldNode(String node){
        realNodes.remove(node);
        for(int i=0;i<Virtual_nodes;i++){
            String virtualNodeName=node+"&&VN"+String.valueOf(i);
            int hash=getHash(virtualNodeName);
            virtualNodes.remove(hash);
        }
    }
}

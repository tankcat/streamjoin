package tool.loadbalance;

import tool.FileProcess;
import util.RouteTableItem;
import util.SystemParameters;
import util.loadbalance.MigrationItem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by stream on 16-9-27.
 */
public class PlanGeneratorSplitRandom implements Serializable{

    public static HashMap<Integer,Integer> boltLoad=new HashMap<>();
    public static HashMap<Integer,Integer> boltOverLoad=new HashMap<>();
    public static HashMap<Integer,HashMap<Integer,Integer>> taskLoadDetail=new HashMap<>();
    public static HashMap<Integer,HashMap<Integer,Integer>> taskOverLoadDetail=new HashMap<>();
    public static HashMap<Integer,ArrayList<MigrationItem>> migrationPlan =new HashMap<>();
    public static HashMap<Integer,ArrayList<RouteTableItem>> routeTable=new HashMap<>();
    public static HashMap<Integer,ArrayList<RouteTableItem>> preRouteTable=new HashMap<>();
    public static int loadSum;
    public static int UL;
    public static int BL;
    public static HashMap<Integer,Integer> bufferC=new HashMap<>();
    public static HashMap<Integer,Integer> endCheckInfo=new HashMap<>();

    public static void update(int index,int load,HashMap<Integer,Integer> keyDetails){
        boltLoad.put(index,load);
        taskLoadDetail.put(index,keyDetails);
        loadSum+=load;
    }

    public static boolean preprocess(){
        Iterator<Map.Entry<Integer,Integer>> taskLoadIter= boltLoad.entrySet().iterator();
        //划分出过载和低载的节点
        boolean flag=false;
        String str="";
        while(taskLoadIter.hasNext()){
            Map.Entry<Integer,Integer> loadItem=(Map.Entry<Integer,Integer>) taskLoadIter.next();
            int index=loadItem.getKey();
            int load=loadItem.getValue();
            if(load>UL){
                flag=true;
                str+=index+"  ";
                boltOverLoad.put(index,load);
                taskOverLoadDetail.put(index,taskLoadDetail.get(index));
                taskLoadIter.remove();
                taskLoadDetail.remove(index);
            }
        }
        System.out.println(str+"号bolt超载了");
        return flag;
    }

    public static void getNewRouteTable(){
        boltLoad.putAll(boltOverLoad);
        taskLoadDetail.putAll(taskOverLoadDetail);
        for(Map.Entry<Integer,ArrayList<RouteTableItem>> preRouteItem:preRouteTable.entrySet()){
            int key=preRouteItem.getKey();
            ArrayList<RouteTableItem> list=preRouteItem.getValue();
            for(RouteTableItem rti:list){
                int keyLoad=taskLoadDetail.get(rti.getTaskIndex()).get(key);
                addToRouteTable(key,keyLoad,rti.getTaskIndex());
            }
        }
    }

    public static boolean makePlanSplitRandom(){
        BL=loadSum/boltLoad.size()+1;
        UL=(int)(BL*(1+SystemParameters.theta)+1);
        boolean flag=preprocess();
        if(flag==false)
            return flag;
        doWithOverloadedTask();
        getNewRouteTable();
        return flag;
    }

    public static void doWithOverloadedTask(){
        preRouteTable=routeTable;
        routeTable=new HashMap<>();
        for(Map.Entry<Integer,Integer> item:boltOverLoad.entrySet()){
            int index=item.getKey();
            int load=item.getValue();
            doUnloadAndLoad(index,load);
        }
    }

    public static void doUnloadAndLoad(int taskIndex, int taskLoad){
        //临时存储
        Iterator<Map.Entry<Integer,Integer>> keyDetailsIter=taskOverLoadDetail.get(taskIndex).entrySet().iterator();
        while(taskLoad>BL&&keyDetailsIter.hasNext()){
            Map.Entry<Integer,Integer> keyDetailsEntry=keyDetailsIter.next();
            int key=keyDetailsEntry.getKey();
            int load=keyDetailsEntry.getValue();
            int delta=taskLoad-BL;
            //若差距大于单个key的负载，则将key整体迁移
            if(delta>=load){
                taskLoad-=load;
                bufferC.put(key,load);
                keyDetailsIter.remove();
            }else{
                taskLoad-=delta;
                bufferC.put(key,delta);
                keyDetailsEntry.setValue(load-delta);
                addToRouteTable(key,load-delta,taskIndex);
            }
            releteFromPreTable(key,taskIndex);
        }
        boltOverLoad.put(taskIndex,taskLoad);
        Iterator<Map.Entry<Integer,Integer>> boltLoadIter=boltLoad.entrySet().iterator();
        while(boltLoadIter.hasNext()){
            Map.Entry<Integer,Integer> loadEntry=boltLoadIter.next();
            int lowIndex=loadEntry.getKey();
            int lowLoad=loadEntry.getValue();
            Iterator<Map.Entry<Integer,Integer>> cIter=bufferC.entrySet().iterator();
            while(cIter.hasNext()&&lowLoad<BL){
                Map.Entry<Integer,Integer> cEntry=cIter.next();
                int key=cEntry.getKey();
                int keyLoad=cEntry.getValue();
                int delta=BL-lowLoad;
                int temp=0;
                if(delta>=keyLoad){
                    lowLoad+=keyLoad;
                    temp=keyLoad;
                    cIter.remove();
                }else{
                    lowLoad+=delta;
                    temp=delta;
                    cEntry.setValue(keyLoad-delta);
                }
                if(endCheckInfo.containsKey(lowIndex)){
                    endCheckInfo.put(lowIndex,endCheckInfo.get(lowIndex)+1);
                }else{
                    endCheckInfo.put(lowIndex,1);
                }
                if(migrationPlan.containsKey(taskIndex)){
                    migrationPlan.get(taskIndex).add(new MigrationItem(lowIndex,key,temp));
                }else{
                    ArrayList<MigrationItem> list=new ArrayList<>();
                    list.add(new MigrationItem(lowIndex,key,temp));
                    migrationPlan.put(taskIndex,list);
                }
                if(taskLoadDetail.get(lowIndex).containsKey(key)){
                    temp+=taskLoadDetail.get(lowIndex).get(key);
                }
                taskLoadDetail.get(lowIndex).put(key,temp);
                addToRouteTable(key,temp,lowIndex);
                releteFromPreTable(key,lowIndex);
            }
            loadEntry.setValue(lowLoad);
        }
    }

    public static void initialize(){
        boltOverLoad.clear();
        taskOverLoadDetail.clear();
    }

    public static void reset(){
        loadSum=0;
        BL=0;
        UL=0;
        boltLoad.clear();
        boltOverLoad.clear();
        taskLoadDetail.clear();
        taskOverLoadDetail.clear();
        preRouteTable.clear();
        //routeTable.clear();
        bufferC.clear();
        endCheckInfo.clear();
        migrationPlan.clear();
    }

    public static void releteFromPreTable(int key,int taskIndex){
        if(preRouteTable.containsKey(key)) {
            ArrayList<RouteTableItem> list = preRouteTable.get(key);
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i).getTaskIndex() == taskIndex) {
                    list.remove(i);
                    break;
                }
            }
            if (list.size() == 0) {
                preRouteTable.remove(key);
            }
        }
    }

    public static void addToRouteTable(int key,int load,int taskIndex){
        if(routeTable.containsKey(key)){
            for(int i=0;i<routeTable.get(key).size();i++){
                if(routeTable.get(key).get(i).getTaskIndex()==taskIndex){
                    int temp=routeTable.get(key).get(i).getCount()+load;
                    routeTable.get(key).get(i).setCount(temp);
                    return;
                }
            }
            routeTable.get(key).add(new RouteTableItem(load,taskIndex));
        }else{
            ArrayList<RouteTableItem> list=new ArrayList<>();
            list.add(new RouteTableItem(load,taskIndex));
            routeTable.put(key,list);
        }
    }

    public static int getOwnerHashIndex(int key){
        return key%SystemParameters.DBOLT_PARA_INIT;
    }

    public static void loadDataFromSingleFile(String fileName){
        BufferedReader br= FileProcess.getReader(fileName);
        String line;
        try {
            while((line=br.readLine())!=null){
                loadSum++;
                int key=Integer.parseInt(line);
                int ownerIndex=getOwnerHashIndex(key);
                if(routeTable.containsKey(key)){
                    ownerIndex=getDestination(key);
                }
                if(boltLoad.containsKey(ownerIndex)){
                    boltLoad.put(ownerIndex,boltLoad.get(ownerIndex)+1);
                    if(taskLoadDetail.get(ownerIndex).containsKey(key)){
                        int temp=taskLoadDetail.get(ownerIndex).get(key);
                        taskLoadDetail.get(ownerIndex).put(key,temp+1);
                    }else{
                        taskLoadDetail.get(ownerIndex).put(key,1);
                    }
                }else{
                    boltLoad.put(ownerIndex,1);
                    HashMap<Integer,Integer> keyload=new HashMap<>();
                    keyload.put(key,1);
                    taskLoadDetail.put(ownerIndex,keyload);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int getDestination(int key){
        Random rand=new Random();
        List<RouteTableItem> list = routeTable.get(key);
        Collections.sort(list);
        List<Integer> temp = new ArrayList<>();
        int sum = list.get(0).getCount();
        temp.add(list.get(0).getCount());
        for (int i = 1; i < list.size(); i++) {
            temp.add(sum + list.get(i).getCount());
            sum += list.get(i).getCount();
        }
        int randNum = rand.nextInt(sum);
        for (int i = 0; i < temp.size(); i++) {
            if (randNum <= temp.get(i)) {
                return list.get(i).getTaskIndex();
            }
        }
        throw new RuntimeException("错误的目标节点！");
    }

    public static void writeLoadToFile(String str,String dir){
        if(!str.contains("after")){
            System.out.println("迁移之前：");
        }else{
            System.out.println("迁移之后：");
        }
        for(Map.Entry<Integer,HashMap<Integer,Integer>> keyLoads:taskLoadDetail.entrySet()){
            int taskIndex=keyLoads.getKey();
            BufferedWriter bw=FileProcess.getWriter("Experiment/Load/"+dir+"/task"+taskIndex+str+".txt");
            int taskLoad=boltLoad.get(taskIndex);
            FileProcess.write("taskIndex = "+taskIndex+" , load = "+taskLoad,bw);
            //System.out.println("taskIndex = "+taskIndex+" , load = "+taskLoad);
            HashMap<Integer,Integer> keys=keyLoads.getValue();
            for(Map.Entry<Integer,Integer> item:keys.entrySet()){
                int key=item.getKey();
                int count=item.getValue();
                FileProcess.write("key = "+key+" , count = "+count,bw);
            }
            FileProcess.close(bw);
        }
    }

    public static void writeTableToFile(String fileName){
        BufferedWriter bw=FileProcess.getWriter(fileName);
        for(Map.Entry<Integer,ArrayList<RouteTableItem>> rti:routeTable.entrySet()){
            int key=rti.getKey();
            ArrayList<RouteTableItem> list=rti.getValue();
            String str="key = "+key+" , ";
            for(RouteTableItem item:list){
                str+=item.toString()+"  ";
            }
            FileProcess.write(str,bw);
        }
        FileProcess.close(bw);
    }
    public static void writeCheckInfo(BufferedWriter bw){
        for(Map.Entry<Integer,Integer> entry:endCheckInfo.entrySet()){
            FileProcess.write("task = "+entry.getKey()+" , number = "+entry.getValue(),bw);
        }
        FileProcess.write("\n",bw);
    }
    public static void writeLoadToFile(BufferedWriter bw,int time){
        FileProcess.write("第"+time+"次监测负载:",bw);
        for(Map.Entry<Integer,HashMap<Integer,Integer>> keyLoads:taskLoadDetail.entrySet()){
            int taskIndex=keyLoads.getKey();
            int taskLoad=boltLoad.get(taskIndex);
            FileProcess.write("taskIndex = "+taskIndex+" , load = "+taskLoad,bw);
            HashMap<Integer,Integer> keys=keyLoads.getValue();
            for(Map.Entry<Integer,Integer> item:keys.entrySet()){
                int key=item.getKey();
                int count=item.getValue();
                FileProcess.write("key = "+key+" , count = "+count,bw);
            }
            FileProcess.write("",bw);
        }
        FileProcess.write("\n",bw);
    }

    public static void writeTableToFile(BufferedWriter bw,int time){
        FileProcess.write("第"+time+"次调整",bw);
        for(Map.Entry<Integer,ArrayList<RouteTableItem>> rti:routeTable.entrySet()){
            int key=rti.getKey();
            ArrayList<RouteTableItem> list=rti.getValue();
            String str="key = "+key+" , ";
            for(RouteTableItem item:list){
                str+=item.toString()+"  ";
            }
            FileProcess.write(str,bw);
        }
        FileProcess.write("\n",bw);
    }

    public static void writeMigratePlan(BufferedWriter bw){
        for(Map.Entry<Integer,ArrayList<MigrationItem>> mpi:migrationPlan.entrySet()){
            int task=mpi.getKey();
            ArrayList<MigrationItem> list=mpi.getValue();
            FileProcess.write("从"+task+"号节点往外迁移:",bw);
            for(MigrationItem mi:list){
                FileProcess.write("\t\t迁往"+mi.getTo()+", key = "+mi.getKey()+", count = "+mi.getCount(),bw);
            }
        }
        FileProcess.write("\n",bw);
    }
    public static void testQMMP(){
        loadDataFromSingleFile("DataSource/keys_48w_150_1_0.txt");
        writeLoadToFile("","QMMP");
        makePlanSplitRandom();
        writeTableToFile("Experiment/Load/tableRandom.txt");
        writeLoadToFile("_after","QMMP");
        for(Map.Entry<Integer,ArrayList<MigrationItem>> mpi:migrationPlan.entrySet()){
            int task=mpi.getKey();
            ArrayList<MigrationItem> list=mpi.getValue();
            System.out.println("从"+task+"号节点往外迁移:");
            for(MigrationItem mi:list){
                System.out.println("迁往"+mi.getTo()+", key = "+mi.getKey()+", count = "+mi.getCount());
            }
            System.out.println();
        }
    }

    public static List<Map.Entry<Integer, Integer>> getLeastKeys(HashMap<Integer,Integer> keys){
        ArrayList<Integer> list=new ArrayList<>();
        List<Map.Entry<Integer, Integer>> values = new ArrayList<>(keys.entrySet());
        Collections.sort(values, new Comparator<Map.Entry<Integer, Integer>>() {
            @Override
            public int compare(Map.Entry<Integer, Integer> t1, Map.Entry<Integer, Integer> t2) {
                return t2.getValue().compareTo(t1.getValue());
            }
        });
        return values;
    }

    public static void main(String[] args){
        testQMMP();
    }

}


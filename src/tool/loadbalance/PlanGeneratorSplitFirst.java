package tool.loadbalance;

import tool.FileProcess;
import util.RouteTableItem;
import util.SystemParameters;
import util.loadbalance.MigrationItem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by stream on 16-10-31.
 * 尽量拆分
 */
public class PlanGeneratorSplitFirst {
    public static HashMap<Integer,Integer> boltLoad=new HashMap<>();
    public static HashMap<Integer,Integer> boltOverLoad=new HashMap<>();
    public static HashMap<Integer,HashMap<Integer,Integer>> taskLoadDetail=new HashMap<>();
    public static HashMap<Integer,HashMap<Integer,Integer>> taskOverLoadDetail=new HashMap<>();
    public static HashMap<Integer,ArrayList<MigrationItem>> migrationPlan =new HashMap<>();
    public static HashMap<Integer,ArrayList<RouteTableItem>> routeTable=new HashMap<>();
    public static HashMap<Integer,ArrayList<RouteTableItem>> preRouteTable=new HashMap<>();
    public static HashMap<Integer,Integer> bufferC=new HashMap<>();
    public static int loadSum;
    public static int UL;
    public static int BL;
    public static HashMap<Integer,Integer> endCheckInfo=new HashMap<>();

    public static void update(int index,int load,HashMap<Integer,Integer> keyDetails){
        boltLoad.put(index,load);
        taskLoadDetail.put(index,keyDetails);
        loadSum+=load;
    }

    public static boolean makeplanMNRT(){
        BL=loadSum/ boltLoad.size()+1;
        UL=(int)(BL*(1+ SystemParameters.theta)+1);
        //System.out.println("BL = "+BL+" , UL = "+UL);
        boolean flag=preprocess();
        if(flag==false){
            return flag;
        }
        doWithOverLoadMNRT();
        getNewRouteTable();
        return flag;
    }

    public static void doWithOverLoadMNRT(){
        preRouteTable=routeTable;
        routeTable=new HashMap<>();
        for(Map.Entry<Integer,Integer> item: boltOverLoad.entrySet()){
            int index=item.getKey();
            int load=item.getValue();
            doUnloadAndLoadMNRT(index,load);
        }

    }

    private static void doUnloadAndLoadMNRT(int taskIndex, int taskLoad){
        UnloadKeys(taskIndex,taskLoad);
        LoadKeys(taskIndex);
        bufferC=new HashMap<>();
    }

    private static void UnloadKeys(int taskIndex,int taskLoad){
        //先将key按照负载从大到小排序
        List<Map.Entry<Integer,Integer>> sortedKeys=getSortedKeys(taskOverLoadDetail.get(taskIndex),true);
        Iterator<Map.Entry<Integer,Integer>> keyIter=sortedKeys.iterator();
        while(taskLoad>BL&&keyIter.hasNext()){
            Map.Entry<Integer,Integer> keyEntry=keyIter.next();
            int key=keyEntry.getKey();
            int keyLoad=keyEntry.getValue();
            //将key拆分
            if(taskLoad-BL<keyLoad){
                bufferC.put(key,taskLoad-BL);
                taskOverLoadDetail.get(taskIndex).put(key,keyLoad-taskLoad+BL);
                addToRouteTable(key,keyLoad-taskLoad+BL,taskIndex);
                taskLoad=BL;
                boltOverLoad.put(taskIndex,taskLoad);
            }else{
                bufferC.put(key,keyLoad);
                taskLoad-=keyLoad;
                taskOverLoadDetail.get(taskIndex).remove(key);
            }
            releteFromPreTable(key,taskIndex);
        }

    }

    private static void LoadKeys(int taskIndex){
        Iterator<Map.Entry<Integer,Integer>> bufferIter=bufferC.entrySet().iterator();
        while(bufferIter.hasNext()){
            Map.Entry<Integer,Integer> keyEntry=bufferIter.next();
            int key=keyEntry.getKey();
            int keyLoad=keyEntry.getValue();
            List<Map.Entry<Integer, Integer>> sortTasks=getSortedKeys(boltLoad,true);
            for(Map.Entry<Integer,Integer> sortTaskEntry:sortTasks){
                int task=sortTaskEntry.getKey();
                int load=sortTaskEntry.getValue();
                int temp=0;
                if(load+keyLoad<=BL){
                    bufferIter.remove();
                    boltLoad.put(task,load+keyLoad);
                    if(taskLoadDetail.get(task).containsKey(key)){
                        temp=taskLoadDetail.get(task).get(key);
                        taskLoadDetail.get(task).put(key,temp+keyLoad);
                    }else{
                        taskLoadDetail.get(task).put(key,keyLoad);
                    }
                    if(endCheckInfo.containsKey(task)){
                        endCheckInfo.put(task,endCheckInfo.get(task)+1);
                    }else{
                        endCheckInfo.put(task,1);
                    }
                    addToRouteTable(key,temp+keyLoad,task);
                    if(migrationPlan.containsKey(taskIndex)){
                        migrationPlan.get(taskIndex).add(new MigrationItem(task,key,keyLoad));
                    }else{
                        ArrayList<MigrationItem> l=new ArrayList<>();
                        l.add(new MigrationItem(task,key,keyLoad));
                        migrationPlan.put(taskIndex,l);
                    }
                    releteFromPreTable(key,task);
                    break;
                }else{
                    if(load>=BL)
                        continue;
                    keyLoad-=(BL-load);
                    boltLoad.put(task,BL);
                    if(taskLoadDetail.get(task).containsKey(key)){
                       temp=taskLoadDetail.get(task).get(key);
                        taskLoadDetail.get(task).put(key,temp+BL-load);
                    }else{
                        taskLoadDetail.get(task).put(key,BL-load);
                    }
                    releteFromPreTable(key,task);
                    addToRouteTable(key,temp+BL-load,task);
                    if(endCheckInfo.containsKey(task)){
                        endCheckInfo.put(task,endCheckInfo.get(task)+1);
                    }else{
                        endCheckInfo.put(task,1);
                    }
                    if(migrationPlan.containsKey(taskIndex)){
                        migrationPlan.get(taskIndex).add(new MigrationItem(task,key,BL-load));
                    }else{
                        ArrayList<MigrationItem> l=new ArrayList<>();
                        l.add(new MigrationItem(task,key,BL-load));
                        migrationPlan.put(taskIndex,l);
                    }
                }

            }
        }
    }

    public static List<Map.Entry<Integer, Integer>> getSortedKeys(HashMap<Integer,Integer> keys,boolean flag){
        ArrayList<Integer> list=new ArrayList<>();
        List<Map.Entry<Integer, Integer>> values = new ArrayList<>(keys.entrySet());
        if(flag==true){
            Collections.sort(values, new Comparator<Map.Entry<Integer, Integer>>() {
                @Override
                public int compare(Map.Entry<Integer, Integer> t1, Map.Entry<Integer, Integer> t2) {
                    return t2.getValue().compareTo(t1.getValue());
                }
            });
        }else{
            Collections.sort(values, new Comparator<Map.Entry<Integer, Integer>>() {
                @Override
                public int compare(Map.Entry<Integer, Integer> t1, Map.Entry<Integer, Integer> t2) {
                    return t1.getValue().compareTo(t2.getValue());
                }
            });
        }
        return values;
    }

    private static boolean preprocess(){
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

    private static void getNewRouteTable(){
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
        endCheckInfo.clear();
        migrationPlan.clear();
        bufferC.clear();
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
            BufferedWriter bw=FileProcess.getWriter("DataSource/Test/"+dir+"/task"+taskIndex+str+".txt");
            int taskLoad=boltLoad.get(taskIndex);
            FileProcess.write("taskIndex = "+taskIndex+" , load = "+taskLoad,bw);
            System.out.println("taskIndex = "+taskIndex+" , load = "+taskLoad);
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

    public static void writeCheckInfo(BufferedWriter bw){
        for(Map.Entry<Integer,Integer> entry:endCheckInfo.entrySet()){
            FileProcess.write("task = "+entry.getKey()+" , number = "+entry.getValue(),bw);
        }
        FileProcess.write("\n",bw);
    }

    public static void testMNRT(){
        loadDataFromSingleFile("DataSource/keys_48w_150_1_0.txt");
        writeLoadToFile("","MNRT");
        makeplanMNRT();
        writeTableToFile("DataSource/Test/MNRT/tableFirst.txt");
        writeLoadToFile("_after","MNRT");
        for(Map.Entry<Integer,ArrayList<MigrationItem>> mpi: migrationPlan.entrySet()){
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
        /*int sum=0;
        for(Map.Entry<Integer,Integer> entry:values){
            Integer key=entry.getKey();
            Integer load=entry.getValue();
            if(sum+load<bound){
                sum+=load;
                list.add(key);
            }
        }
        for(Integer key:list){
            System.out.println("key = "+key+", load = "+keys.get(key));
        }*/
        return values;
    }

    public static void main(String[] args){
        testMNRT();
    }
}

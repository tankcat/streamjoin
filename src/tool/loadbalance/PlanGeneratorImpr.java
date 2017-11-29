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
 * Created by stream on 16-10-10.
 */
public class PlanGeneratorImpr {
    public static int TaskPara=5;
    public static HashMap<Integer,Integer> boltLoad=new HashMap<>();
    public static HashMap<Integer,Integer> boltOverLoad=new HashMap<>();
    public static HashMap<Integer,HashMap<Integer,Integer>> taskLoadDetail=new HashMap<>();
    public static HashMap<Integer,HashMap<Integer,Integer>> taskOverLoadDetail=new HashMap<>();
    public static ArrayList<MigrationItem> migratePlan=new ArrayList<>();
    public static HashMap<Integer,ArrayList<RouteTableItem>> routeTable=new HashMap<>();
    public static HashMap<Integer,ArrayList<RouteTableItem>> preRouteTable=new HashMap<>();
    public static HashMap<Integer,Integer> keyDetailsInTable=new HashMap<>();
    public static int loadSum;
    public static int UL;
    public static int BL;
    public static int[] largekey=new int[3];

    public static void update(int index,int load,HashMap<Integer,Integer> keyDetails){
        boltLoad.put(index,load);
        taskLoadDetail.put(index,keyDetails);
        loadSum+=load;
    }

    /**
     * 超载节点的迁移情况:
     * 1. key 不在路由表中
     *   ①. 整体迁出到非本节点
     *   ②. 拆分迁出到非本节点
     * 2. key 在路由表中
     *   (1). key只存在于一个节点中
     *      ①. 整体回迁到本节点
     *      ②. 整体移迁到非本节点
     *      ③. 拆分移迁到非本节点
     *   (2). key存在于不止一个节点中
     *      ①. 当前节点是本节点
     *          a. 合并迁出(整体)到路由表中的其余节点
     *          b. 合并迁出(整体)到路由表外的其余节点
     *          c. 拆分迁出到路由表中的其余节点
     *          d. 拆分迁出到路由表外的其余节点
     *      ②. 当前节点是非本节点
     *          a. 合并回迁(整体)到路由表中的本节点
     *          b. 合并移迁(整体)到路由表外的其余节点
     *          c. 拆分移迁到路由表中的其余节点
     *          d. 拆分移迁到路由表中的其余节点
     *
     * @return
     */

    public static boolean makeplanMNRT(){
        BL=loadSum/ boltLoad.size()+1;
        UL=(int)(BL*(1+ SystemParameters.theta)+1);
        System.out.println("BL = "+BL+" , UL = "+UL);
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
        //区分出是否存在于路由表中的key
            keyDetailsInTable = new HashMap<>();
            Set<Integer> keyInTable = preRouteTable.keySet();
            if (keyInTable != null) {
                for (Integer key : keyInTable) {
                    if (taskOverLoadDetail.get(taskIndex).containsKey(key)) {
                        keyDetailsInTable.put(key, taskOverLoadDetail.get(taskIndex).get(key));
                        taskOverLoadDetail.get(taskIndex).remove(key);
                    }
                }
            }
            //优先处理处于路由表中的key
            if (keyDetailsInTable.size() > 0) {
                taskLoad = processKeysInTable(taskIndex, taskLoad);
            }
            //如果仍然超载，则处理不在路由表中的key
            if (taskLoad > BL) {
                taskLoad = processKeysNotInTable(taskIndex, taskLoad);
            }
        System.out.println("拆分之前："+taskLoad);
        //将路由表中的key与非路由表中的key混合起来，重新处理
        if(taskLoad>BL) {
            //最后统一处理拆分的key
            processKeysToSplit(taskIndex,taskLoad);
        }
        taskOverLoadDetail.get(taskIndex).putAll(keyDetailsInTable);
    }

    public static int processKeysInTable(int taskIndex,int taskLoad){
        //List<Map.Entry<Integer,Integer>> keys=getLeastKeys(keyDetailsInTable);
        Iterator<Map.Entry<Integer,Integer>> keyDetailsIter=keyDetailsInTable.entrySet().iterator();
        boolean flag=true;
        while(keyDetailsIter.hasNext()) {
            flag=true;
            Map.Entry<Integer,Integer> keyDetailsEntry=keyDetailsIter.next();
            int key = keyDetailsEntry.getKey();
            int load = keyDetailsEntry.getValue();
            //路由表中该key只有一个路由项
            if (preRouteTable.get(key).size() == 1) {
                for (Map.Entry<Integer, Integer> index : boltLoad.entrySet()) {
                    if (load + index.getValue() <= BL && taskLoad-load>=BL) {
                        taskLoad -= load;
                        index.setValue(index.getValue() + load);
                        preRouteTable.remove(key);
                        keyDetailsIter.remove();
                        keyDetailsInTable.remove(key);
                        taskLoadDetail.get(index.getKey()).put(key,load);
                        //移迁到非本节点
                        if (getOwnerHashIndex(key) != index.getKey()) {
                            addToRouteTable(key,load,index.getKey());
                        }
                        break;
                    }
                }
            } else {
                int ownerIndex = getOwnerHashIndex(key);
                ArrayList<RouteTableItem> list = preRouteTable.get(key);

                //优先考虑本节点,且本节点的空间足够大(本节点不在路由表中)
                if (boltLoad.containsKey(ownerIndex) && boltLoad.get(ownerIndex) + load <= BL && taskLoad-load>=BL) {
                    taskLoad-=load;
                    boltLoad.put(ownerIndex,boltLoad.get(ownerIndex)+load);
                    int temp=taskLoadDetail.get(ownerIndex).get(key)+load;
                    taskLoadDetail.get(ownerIndex).put(key,temp);
                    keyDetailsIter.remove();
                    keyDetailsInTable.remove(key);
                    addToRouteTable(key,load,ownerIndex);
                    releteFromPreTable(key,taskIndex);
                    continue;
                }
                //其次考虑路由表中的项
                Iterator<RouteTableItem> iter = list.iterator();
                while (iter.hasNext()) {
                    RouteTableItem item = iter.next();
                    if (boltLoad.containsKey(item.getTaskIndex()) && boltLoad.get(item.getTaskIndex()) + load <= BL && taskLoad-load>=BL) {
                        taskLoad-=load;
                        boltLoad.put(item.getTaskIndex(),boltLoad.get(item.getTaskIndex())+load);
                        int temp=load+taskLoadDetail.get(item.getTaskIndex()).get(key);
                        taskLoadDetail.get(item.getTaskIndex()).put(key,temp);
                        keyDetailsIter.remove();
                        keyDetailsInTable.remove(key);
                        iter.remove();
                        releteFromPreTable(key,taskIndex);
                        flag=false;
                        break;
                    }
                }
                //最后考虑路由表之外的项
                if(flag==true) {
                    for (Map.Entry<Integer, Integer> index : boltLoad.entrySet()) {
                        if (index.getValue() + load <= BL && taskLoad-load>=BL) {
                            taskLoad -= load;
                            index.setValue(index.getValue() + load);
                            taskLoadDetail.get(index.getKey()).put(key, load);
                            keyDetailsIter.remove();
                            keyDetailsInTable.remove(key);
                            addToRouteTable(key,load,index.getKey());
                            releteFromPreTable(key,taskIndex);
                            break;
                        }
                    }
                }

            }

        }
        return taskLoad;
    }

    public static int processKeysNotInTable(int taskIndex,int taskLoad){
       // List<Map.Entry<Integer,Integer>> keys=getLeastKeys(taskOverLoadDetail.get(taskIndex));
        Iterator<Map.Entry<Integer,Integer>> keyNotInTableIter=taskOverLoadDetail.get(taskIndex).entrySet().iterator();
        //优先考虑可以整体迁出的
        while(taskLoad>BL && keyNotInTableIter.hasNext()){
            Map.Entry<Integer,Integer> notInTableItem=keyNotInTableIter.next();
            int key=notInTableItem.getKey();
            int load=notInTableItem.getValue();
            for(Map.Entry<Integer,Integer> index:boltLoad.entrySet()){
                if(load+index.getValue()<=BL && taskLoad-load>=BL){
                    taskLoad-=load;
                    taskLoadDetail.get(index.getKey()).put(key,load);
                    boltOverLoad.put(taskIndex,taskLoad);
                    keyNotInTableIter.remove();
                    taskOverLoadDetail.get(taskIndex).remove(key);
                    //taskOverLoadDetail.remove(key);
                    addToRouteTable(key,load,index.getKey());
                    index.setValue(index.getValue()+load);
                    break;
                }
            }
        }
        return taskLoad;
    }

    public static void processKeysToSplit(int taskIndex,int taskLoad) {
        //临时存储
        HashMap<Integer, RouteTableItem> C = new HashMap<>();
            //先拆分在路由表中的key
       // List<Map.Entry<Integer, Integer>> keys = getLeastKeys(keyDetailsInTable);
        Iterator<Map.Entry<Integer, Integer>> keyInTableIter = keyDetailsInTable.entrySet().iterator();
        while (taskLoad > BL && keyInTableIter.hasNext()) {
            Map.Entry<Integer, Integer> keyDetailsEntry = keyInTableIter.next();
            int key = keyDetailsEntry.getKey();
            int load = keyDetailsEntry.getValue();
            int delta = taskLoad - BL;
            if (delta >= load) {
                taskLoad -= load;
                C.put(key, new RouteTableItem(load, taskIndex));
                releteFromPreTable(key, taskIndex);
                keyInTableIter.remove();
                keyDetailsInTable.remove(key);
            } else {
                // delta<load
                taskLoad -= delta;
                C.put(key, new RouteTableItem(delta, taskIndex));
                for (int i = 0; i < preRouteTable.get(key).size(); i++) {
                    if (preRouteTable.get(key).get(i).getTaskIndex() == taskIndex) {
                        int temp = load - delta;
                        preRouteTable.get(key).get(i).setCount(temp);
                        System.out.println();
                    }
                }
                keyDetailsEntry.setValue(load - delta);
                keyDetailsInTable.put(key, load - delta);
            }
        }
        //再拆分不在路由表中的key
        //keys = getLeastKeys(taskOverLoadDetail.get(taskIndex));
        Iterator<Map.Entry<Integer, Integer>> keyNotInTableIter = taskOverLoadDetail.get(taskIndex).entrySet().iterator();
        while (taskLoad > BL && keyNotInTableIter.hasNext()) {
            Map.Entry<Integer, Integer> keyDetailsEntry = keyNotInTableIter.next();
            int key = keyDetailsEntry.getKey();
            int load = keyDetailsEntry.getValue();
            int delta = taskLoad - BL;
            if((double)delta/BL<0.001)
                continue;
            if (delta >= load) {
                taskLoad -= load;
                C.put(key, new RouteTableItem(load, taskIndex));
                keyNotInTableIter.remove();
                taskOverLoadDetail.get(taskIndex).remove(key);
            } else {
                taskLoad -= delta;
                C.put(key, new RouteTableItem(delta, taskIndex));
                keyDetailsEntry.setValue(load - delta);
                taskOverLoadDetail.get(taskIndex).put(key, load - delta);
                addToRouteTable(key, load - delta, taskIndex);
            }
        }
        boltOverLoad.put(taskIndex,taskLoad);
        Iterator<Map.Entry<Integer,Integer>> boltLoadIter=boltLoad.entrySet().iterator();
        while(boltLoadIter.hasNext()){
            Map.Entry<Integer,Integer> loadEntry=boltLoadIter.next();
            int lowIndex=loadEntry.getKey();
            int lowLoad=loadEntry.getValue();
            Iterator<Map.Entry<Integer,RouteTableItem>> cIter=C.entrySet().iterator();
            while(lowLoad<BL&&cIter.hasNext()){
                Map.Entry<Integer,RouteTableItem> cEntry=cIter.next();
                int key=cEntry.getKey();
                RouteTableItem rti=cEntry.getValue();
                int delta=BL-lowLoad;
                int temp=0;
                if(delta>=rti.getCount()){
                    lowLoad+=rti.getCount();
                    temp=rti.getCount();
                    cIter.remove();
                }else{
                    lowLoad+=delta;
                    temp=delta;
                    cEntry.setValue(new RouteTableItem(rti.getCount()-delta,rti.getTaskIndex()));
                    //System.out.println("key = "+key+", splitOut= "+delta+" , dest = "+lowIndex);
                }
                if(taskLoadDetail.get(lowIndex).containsKey(key)){
                    temp+=taskLoadDetail.get(lowIndex).get(key);
                }
                taskLoadDetail.get(lowIndex).put(key,temp);
                addToRouteTable(key,temp,lowIndex);
            }

            loadEntry.setValue(lowLoad);
        }

        System.out.println("============ Test ===========");
        for(Map.Entry<Integer,RouteTableItem> cItem:C.entrySet()){
            int key=cItem.getKey();
            RouteTableItem rti=cItem.getValue();
            System.out.println("key = "+key +", "+rti.toString());
        }
        System.out.println("=============================");
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

    public static boolean makeplanQMMP(){
        BL=loadSum/boltLoad.size()+1;
        UL=(int)(BL*(1+SystemParameters.theta)+1);
        boolean flag=preprocess();
        if(flag==false)
            return flag;
        doWithOverLoadQMMP();
        getNewRouteTable();
        return flag;
    }

    public static void doWithOverLoadQMMP(){
        preRouteTable=routeTable;
        routeTable=new HashMap<>();
        for(Map.Entry<Integer,Integer> item:boltOverLoad.entrySet()){
            int index=item.getKey();
            int load=item.getValue();
            doUnloadAndLoadQMNP(index,load);
        }
    }

    public static void doUnloadAndLoadQMNP(int taskIndex,int taskLoad){
        //区分出路由表和非路由表中的key
        keyDetailsInTable=new HashMap<>();
        Set<Integer> keyInTable=preRouteTable.keySet();
        if(keyInTable!=null){
            for(Integer key:keyInTable){
                if(taskOverLoadDetail.get(taskIndex).containsKey(key)){
                    keyDetailsInTable.put(key,taskOverLoadDetail.get(taskIndex).get(key));
                    taskOverLoadDetail.get(taskIndex).remove(key);
                }
            }
        }
        //临时存储
        HashMap<Integer,RouteTableItem> C=new HashMap<>();
        //先处理路由表中的key
        Iterator<Map.Entry<Integer,Integer>> keyDetailsInTableIter=keyDetailsInTable.entrySet().iterator();
        while(taskLoad>BL&&keyDetailsInTableIter.hasNext()){
            Map.Entry<Integer,Integer> keyDetailsInTableEntry=keyDetailsInTableIter.next();
            int key=keyDetailsInTableEntry.getKey();
            int load=keyDetailsInTableEntry.getValue();
            int delta=taskLoad-BL;
            if(delta>=load){
                taskLoad-=load;
                C.put(key,new RouteTableItem(load,taskIndex));
                releteFromPreTable(key,taskIndex);
                keyDetailsInTableIter.remove();
            }else{
                taskLoad-=delta;
                C.put(key,new RouteTableItem(delta,taskIndex));
                for(int i=0;i<preRouteTable.get(key).size();i++){
                    if(preRouteTable.get(key).get(i).getTaskIndex()==taskIndex){
                        int temp=load-delta;
                        preRouteTable.get(key).get(i).setCount(temp);
                        break;
                    }
                }
                keyDetailsInTableEntry.setValue(load-delta);
            }
        }
        //再处理非路由表中key
        Iterator<Map.Entry<Integer,Integer>> keyDetailsIter=taskOverLoadDetail.get(taskIndex).entrySet().iterator();
        while(taskLoad>BL&&keyDetailsIter.hasNext()){
            Map.Entry<Integer,Integer> keyDetailsEntry=keyDetailsIter.next();
            int key=keyDetailsEntry.getKey();
            int load=keyDetailsEntry.getValue();
            int delta=taskLoad-BL;
            //若差距大于单个key的负载，则将key整体迁移
            if(delta>=load){
                taskLoad-=load;
                C.put(key,new RouteTableItem(load,taskIndex));
                keyDetailsIter.remove();
            }else{
                taskLoad-=delta;
                C.put(key,new RouteTableItem(delta,taskIndex));
                keyDetailsEntry.setValue(load-delta);
                addToRouteTable(key,load-delta,taskIndex);
            }
        }
        boltOverLoad.put(taskIndex,taskLoad);
        Iterator<Map.Entry<Integer,Integer>> boltLoadIter=boltLoad.entrySet().iterator();
        while(boltLoadIter.hasNext()){
            Map.Entry<Integer,Integer> loadEntry=boltLoadIter.next();
            int lowIndex=loadEntry.getKey();
            int lowLoad=loadEntry.getValue();
            Iterator<Map.Entry<Integer,RouteTableItem>> cIter=C.entrySet().iterator();
            while(cIter.hasNext()){
                Map.Entry<Integer,RouteTableItem> cEntry=cIter.next();
                int key=cEntry.getKey();
                RouteTableItem rti=cEntry.getValue();
                int delta=BL-lowLoad;
                int temp=0;
                if(delta>=rti.getCount()){
                    lowLoad+=rti.getCount();
                    temp=rti.getCount();
                    cIter.remove();
                }else{
                    lowLoad+=delta;
                    temp=delta;
                    cEntry.setValue(new RouteTableItem(rti.getCount()-delta,rti.getTaskIndex()));
                }
                if(taskLoadDetail.get(lowIndex).containsKey(key)){
                    temp+=taskLoadDetail.get(lowIndex).get(key);
                }
                taskLoadDetail.get(lowIndex).put(key,temp);
                addToRouteTable(key,temp,lowIndex);
            }
            loadEntry.setValue(lowLoad);
        }

        System.out.println("============ Test ===========");
        for(Map.Entry<Integer,RouteTableItem> cItem:C.entrySet()){
            int key=cItem.getKey();
            RouteTableItem rti=cItem.getValue();
            System.out.println("key = "+key +", "+rti.toString());
        }
        System.out.println("=============================");
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
        routeTable.clear();
        keyDetailsInTable.clear();

    }

    public static void releteFromPreTable(int key,int taskIndex){
        ArrayList<RouteTableItem> list=preRouteTable.get(key);
        for(int i=0;i<list.size();i++){
            if(list.get(i).getTaskIndex()==taskIndex){
                list.remove(i);
                break;
            }
        }
        if(list.size()==0){
            preRouteTable.remove(key);
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
        return key%TaskPara;
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

    public static void testMNRT(){
        loadDataFromSingleFile("DataSource/keys_48w_150_1_0.txt");
        writeLoadToFile("","MNRT");
        makeplanMNRT();
        writeTableToFile("DataSource/Test/MNRT/table.txt");
        writeLoadToFile("_after","MNRT");

        initialize();

        /*loadDataFromSingleFile("DataSource/keys_0_5.txt");
        writeLoadToFile("_2","MNRT");
        makeplanMNRT();
        writeTableToFile("DataSource/Test/MNRT/table_2.txt");
        writeLoadToFile("_2_after","MNRT");*/
    }

    public static void testQMMP(){
        loadDataFromSingleFile("DataSource/keys_1_5.txt");
        writeLoadToFile("","QMMP");
        makeplanQMMP();
        writeTableToFile("DataSource/Test/QMMP/table.txt");
        writeLoadToFile("_after","QMMP");

        initialize();

        loadDataFromSingleFile("DataSource/keys_0_5.txt");
        writeLoadToFile("_2","QMMP");
        makeplanQMMP();
        writeTableToFile("DataSource/Test/QMMP/table_2.txt");
        writeLoadToFile("_2_after","QMMP");
    }

    public static void main(String[] args){
        /*testQMMP();
        reset();
        System.out.println("----------------------------------------");*/
        testMNRT();

    }
}

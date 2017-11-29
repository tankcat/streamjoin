package tool.loadbalance;

import org.apache.commons.math3.util.Pair;

import tool.FileProcess;
import util.RouteTableItem;
import util.SystemParameters;
import util.loadbalance.MigrationItem;
import util.loadbalance.SelectList;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by stream on 16-10-12.
 */
public class PlanGeneratorSplitLast {
    public static HashMap<Integer,Integer> boltLoad=new HashMap<>();
    public static HashMap<Integer,Integer> boltOverLoad=new HashMap<>();
    public static HashMap<Integer,HashMap<Integer,Integer>> taskLoadDetail=new HashMap<>();
    public static HashMap<Integer,HashMap<Integer,Integer>> taskOverLoadDetail=new HashMap<>();
    public static HashMap<Integer,ArrayList<MigrationItem>> migrationPlan=new HashMap<>();
    public static HashMap<Integer,ArrayList<RouteTableItem>> routeTable=new HashMap<>();
    public static HashMap<Integer,ArrayList<RouteTableItem>> preRouteTable=new HashMap<>();
    public static HashMap<Integer,Integer> keyDetailsInTable=new HashMap<>();
    public static int loadSum;
    public static int UL;
    public static int BL;
    public static int upOrDown=10;
    public static Stack<Pair<Integer,Integer>> resultItem=new Stack<>();
    public static List<ArrayList<Pair<Integer,Integer>>> results=new ArrayList<>();
    public static HashMap<Integer,Integer> endCheckInfo=new HashMap<>();

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
            taskLoad = processKeyNotInTable(taskIndex, taskLoad);
        }
        //System.out.println("拆分之前："+taskLoad);
        //将路由表中的key与非路由表中的key混合起来，重新处理
        if(taskLoad>BL) {
            //最后统一处理拆分的key
            processKeysToSplit(taskIndex,taskLoad);
        }
        taskOverLoadDetail.get(taskIndex).putAll(keyDetailsInTable);
    }

    public static int processKeysInTable(int taskIndex,int taskLoad){
        List<Map.Entry<Integer,Integer>> keys= getSortedKeys(keyDetailsInTable,false);
        Iterator<Map.Entry<Integer,Integer>> keyDetailsIter=keys.iterator();
        boolean flag=true;
        while(keyDetailsIter.hasNext()) {
            flag=true;
            Map.Entry<Integer,Integer> keyDetailsEntry=keyDetailsIter.next();
            int key = keyDetailsEntry.getKey();
            int load = keyDetailsEntry.getValue();
            //路由表中该key只有一个路由项
            String str="";
            if (preRouteTable.get(key).size() == 1) {
                str+="key = "+key+" 在路由表中只有一项";
                List<Map.Entry<Integer,Integer>> sortTasks=getSortedKeys(boltLoad,false);
                for(Map.Entry<Integer,Integer> sortTaskEntry:sortTasks){
                    int lowtask=sortTaskEntry.getKey();
                    int lowload=sortTaskEntry.getValue();
                    if(load+lowload<=BL&& taskLoad-load>=BL){
                        taskLoad-=load;
                        boltLoad.put(lowtask,load+lowload);
                        taskLoadDetail.get(lowtask).put(key,load);
                        preRouteTable.remove(key);
                        keyDetailsIter.remove();
                        keyDetailsInTable.remove(key);
                        if(endCheckInfo.containsKey(lowtask)){
                            endCheckInfo.put(lowtask,endCheckInfo.get(lowtask)+1);
                        }else{
                            endCheckInfo.put(lowtask,1);
                        }
                        if(migrationPlan.containsKey(taskIndex)){
                            migrationPlan.get(taskIndex).add(new MigrationItem(lowtask,key,load));
                        }else{
                            ArrayList<MigrationItem> l=new ArrayList<>();
                            l.add(new MigrationItem(lowtask,key,load));
                            migrationPlan.put(taskIndex,l);
                        }
                        //迁移到非本节点
                        if(getOwnerHashIndex(key)!=lowtask){
                            addToRouteTable(key,load,lowtask);
                            str+=", 移迁到路由表外的"+lowtask+" 号DBolt";
                        }else{
                            str+=", 回迁到本节点 "+getOwnerHashIndex(key)+" 号DBolt";
                        }
                        break;
                    }
                }
                /*for (Map.Entry<Integer, Integer> index : boltLoad.entrySet()) {
                    if (load + index.getValue() <= BL && taskLoad-load>=BL) {
                        taskLoad -= load;
                        index.setValue(index.getValue() + load);
                        preRouteTable.remove(key);
                        keyDetailsIter.remove();
                        keyDetailsInTable.remove(key);
                        taskLoadDetail.get(index.getKey()).put(key,load);
                        if(endCheckInfo.containsKey(index.getKey())){
                            endCheckInfo.put(index.getKey(),endCheckInfo.get(index.getKey())+1);
                        }else{
                            endCheckInfo.put(index.getKey(),1);
                        }
                        if(migrationPlan.containsKey(taskIndex)){
                            migrationPlan.get(taskIndex).add(new MigrationItem(index.getKey(),key,load));
                        }else{
                            ArrayList<MigrationItem> list=new ArrayList<>();
                            list.add(new MigrationItem(index.getKey(),key,load));
                            migrationPlan.put(taskIndex,list);
                        }
                        //移迁到非本节点
                        if (getOwnerHashIndex(key) != index.getKey()) {
                            addToRouteTable(key,load,index.getKey());
                            str+=", 移迁到路由表外的"+index.getKey()+" 号DBolt";
                        }else{
                            str+=", 回迁到本节点 "+getOwnerHashIndex(key)+" 号DBolt";
                        }
                        break;
                    }
                }*/
            } else {
                str+="key = "+key+" 在路由表中不止一项";
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
                    if(endCheckInfo.containsKey(ownerIndex)){
                        endCheckInfo.put(ownerIndex,endCheckInfo.get(ownerIndex)+1);
                    }else{
                        endCheckInfo.put(ownerIndex,1);
                    }
                    releteFromPreTable(key,taskIndex);
                    if(migrationPlan.containsKey(taskIndex)){
                        migrationPlan.get(taskIndex).add(new MigrationItem(ownerIndex,key,load));
                    }else{
                        ArrayList<MigrationItem> l=new ArrayList<>();
                        l.add(new MigrationItem(ownerIndex,key,load));
                        migrationPlan.put(taskIndex,l);
                    }
                    str+=", 可回迁到本节点 "+ownerIndex+" 号DBolt";
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
                        addToRouteTable(key,load,item.getTaskIndex());
                        flag=false;
                        if(endCheckInfo.containsKey(item.getTaskIndex())){
                            endCheckInfo.put(item.getTaskIndex(),endCheckInfo.get(item.getTaskIndex())+1);
                        }else{
                            endCheckInfo.put(item.getTaskIndex(),1);
                        }
                        if(migrationPlan.containsKey(taskIndex)){
                            migrationPlan.get(taskIndex).add(new MigrationItem(item.getTaskIndex(),key,load));
                        }else{
                            ArrayList<MigrationItem> l=new ArrayList<>();
                            l.add(new MigrationItem(item.getTaskIndex(),key,load));
                            migrationPlan.put(taskIndex,l);
                        }
                        str+=", 可移迁到路由表中的 "+item.getTaskIndex()+" 号DBolt";
                        break;
                    }
                }
                //最后考虑路由表之外的项
                if(flag==true) {
                    List<Map.Entry<Integer,Integer>> sortTasks=getSortedKeys(boltLoad,false);
                    for(Map.Entry<Integer,Integer> sortTaskEntry:sortTasks){
                        int lowtask=sortTaskEntry.getKey();
                        int lowload=sortTaskEntry.getValue();
                        if(lowload+load<=BL&&taskLoad-load>=BL){
                            taskLoad -= load;
                            boltLoad.put(lowtask,lowload+load);
                            taskLoadDetail.get(lowtask).put(key,load);
                            keyDetailsIter.remove();
                            keyDetailsInTable.remove(key);
                            addToRouteTable(key,load,lowtask);
                            releteFromPreTable(key,taskIndex);
                            if(endCheckInfo.containsKey(lowtask)){
                                endCheckInfo.put(lowtask,endCheckInfo.get(lowtask)+1);
                            }else{
                                endCheckInfo.put(lowtask,1);
                            }
                            if(migrationPlan.containsKey(taskIndex)){
                                migrationPlan.get(taskIndex).add(new MigrationItem(lowtask,key,load));
                            }else{
                                ArrayList<MigrationItem> l=new ArrayList<>();
                                l.add(new MigrationItem(lowtask,key,load));
                                migrationPlan.put(taskIndex,l);
                            }
                            str+=", 可移迁到路由表外的 "+lowtask+" 号DBolt";
                            break;
                        }
                    }
                    /*for (Map.Entry<Integer, Integer> index : boltLoad.entrySet()) {
                        if (index.getValue() + load <= BL && taskLoad-load>=BL) {
                            taskLoad -= load;
                            index.setValue(index.getValue() + load);
                            taskLoadDetail.get(index.getKey()).put(key, load);
                            keyDetailsIter.remove();
                            keyDetailsInTable.remove(key);
                            addToRouteTable(key,load,index.getKey());
                            releteFromPreTable(key,taskIndex);
                            if(endCheckInfo.containsKey(index.getKey())){
                                endCheckInfo.put(index.getKey(),endCheckInfo.get(index.getKey())+1);
                            }else{
                                endCheckInfo.put(index.getKey(),1);
                            }
                            if(migrationPlan.containsKey(taskIndex)){
                                migrationPlan.get(taskIndex).add(new MigrationItem(index.getKey(),key,load));
                            }else{
                                ArrayList<MigrationItem> l=new ArrayList<>();
                                l.add(new MigrationItem(index.getKey(),key,load));
                                migrationPlan.put(taskIndex,l);
                            }
                            str+=", 可移迁到路由表外的 "+index.getKey()+" 号DBolt";
                            break;
                        }
                    }*/
                }

            }
            System.out.println(str);
        }
        return taskLoad;
    }

    public static int processLittleKeys(List<Pair<Integer,Integer>> keys,int taskIndex,int taskLoad){
        List<Map.Entry<Integer,Integer>> sortTasks=getSortedKeys(boltLoad,true);
        for(int i=0;i<sortTasks.size()-1;i++){
            int sum=0;
            for(int k=0;k<keys.size();k++){
                sum+=keys.get(k).getValue();
            }
            int lowtask=sortTasks.get(i).getKey();
            int lowload=sortTasks.get(i).getValue();
            Collections.sort(keys, (integerIntegerPair, t1) -> t1.getValue()-integerIntegerPair.getValue());
            keys.add(0, new Pair<>(0, 0));
            candidateKeys.clear();
            flag=false;
            int tmp=BL-lowload;
            if(tmp<=0)
                continue;
            search(tmp,keys,sum,true);
            if(candidateKeys.size()==0){
                keys.remove(0);
                Collections.sort(keys, (integerIntegerPair, t1) -> integerIntegerPair.getValue()-t1.getValue());
                keys.add(0, new Pair<>(0, 0));
                flag=false;
                search(tmp,keys,sum,true);
            }
            if(candidateKeys.size()==0){
                //System.out.println("小大无误差，无");
                keys.remove(0);
                Collections.sort(keys, (integerIntegerPair, t1) -> t1.getValue()-integerIntegerPair.getValue());
                keys.add(0, new Pair<>(0, 0));
                flag=false;
                search(tmp,keys,sum,false);
            }
            if(candidateKeys.size()==0){
                //System.out.println("大小有误差，无");
                keys.remove(0);
                Collections.sort(keys, (integerIntegerPair, t1) -> integerIntegerPair.getValue()-t1.getValue());
                keys.add(0, new Pair<>(0, 0));
                flag=false;
                search(tmp,keys,sum,false);
            }
            if(candidateKeys.size()>0){
                //找到序列，直接加入
                for(Pair<Integer,Integer> candidate:candidateKeys){
                    int key=candidate.getKey();
                    int keyLoad=candidate.getValue();
                    taskLoad-=keyLoad;
                    for(int j=0;j<keys.size();j++){
                        if(keys.get(j).getKey()==key){
                            keys.remove(j);
                            break;
                        }
                    }
                    lowload+=keyLoad;
                    taskLoadDetail.get(lowtask).put(key,keyLoad);
                    taskOverLoadDetail.get(taskIndex).remove(key);
                    addToRouteTable(key,keyLoad,lowtask);
                    if(endCheckInfo.containsKey(lowtask)){
                        endCheckInfo.put(lowtask,endCheckInfo.get(lowtask)+1);
                    }else{
                        endCheckInfo.put(lowtask,1);
                    }
                    if(migrationPlan.containsKey(taskIndex)){
                        migrationPlan.get(taskIndex).add(new MigrationItem(lowtask,key,keyLoad));
                    }else{
                        ArrayList<MigrationItem> l=new ArrayList<>();
                        l.add(new MigrationItem(lowtask,key,keyLoad));
                        migrationPlan.put(taskIndex,l);
                    }
                }
                boltLoad.put(lowtask,lowload);
            }else{
                keys.remove(0);
                Collections.sort(keys, (integerIntegerPair, t1) -> integerIntegerPair.getValue()-t1.getValue());
                Iterator<Pair<Integer,Integer>> keysIter=keys.iterator();
                while(lowload<BL&&keysIter.hasNext()){
                    Pair<Integer,Integer> entry=keysIter.next();
                    int key=entry.getKey();
                    int keyLoad=entry.getValue();
                    if(lowload+keyLoad<=BL){
                        keysIter.remove();
                        lowload+=keyLoad;
                        taskLoad-=keyLoad;
                        taskLoadDetail.get(lowtask).put(key,keyLoad);
                        taskOverLoadDetail.get(taskIndex).remove(key);
                        addToRouteTable(key,keyLoad,lowtask);
                        if(endCheckInfo.containsKey(lowtask)){
                            endCheckInfo.put(lowtask,endCheckInfo.get(lowtask)+1);
                        }else{
                            endCheckInfo.put(lowtask,1);
                        }
                        if(migrationPlan.containsKey(taskIndex)){
                            migrationPlan.get(taskIndex).add(new MigrationItem(lowtask,key,keyLoad));
                        }else{
                            ArrayList<MigrationItem> l=new ArrayList<>();
                            l.add(new MigrationItem(lowtask,key,keyLoad));
                            migrationPlan.put(taskIndex,l);
                        }
                    }
                }
                boltLoad.put(lowtask,lowload);
            }
        }
        if(keys.size()>0) {
            int lowtask = sortTasks.get(sortTasks.size() - 1).getKey();
            int lowload = sortTasks.get(sortTasks.size() - 1).getValue();
            for(int i=0;i<keys.size();i++){
                int key=keys.get(i).getKey();
                int keyLoad=keys.get(i).getValue();
                taskLoad-=keyLoad;
                lowload+=keyLoad;
                taskLoadDetail.get(lowtask).put(key,keyLoad);
                taskOverLoadDetail.get(taskIndex).remove(key);
                addToRouteTable(key,keyLoad,lowtask);
                if(endCheckInfo.containsKey(lowtask)){
                    endCheckInfo.put(lowtask,endCheckInfo.get(lowtask)+1);
                }else{
                    endCheckInfo.put(lowtask,1);
                }
                if(migrationPlan.containsKey(taskIndex)){
                    migrationPlan.get(taskIndex).add(new MigrationItem(lowtask,key,keyLoad));
                }else{
                    ArrayList<MigrationItem> l=new ArrayList<>();
                    l.add(new MigrationItem(lowtask,key,keyLoad));
                    migrationPlan.put(taskIndex,l);
                }
            }
            boltLoad.put(lowtask,lowload);
            keys.clear();
        }
        boltOverLoad.put(taskIndex,taskLoad);
        return taskLoad;
    }

    public static int processKeyNotInTable(int taskIndex,int taskLoad){
        int maxDelta=0;
        List<Pair<Integer,Integer>> keysAsTotal = new ArrayList<>();
        List<Pair<Integer,Integer>> taskDelta=new ArrayList<>();
        for(Map.Entry<Integer,Integer> index:boltLoad.entrySet()){
            int delta=BL-index.getValue();
            if(delta>0){
                taskDelta.add(new Pair<Integer, Integer>(index.getKey(),delta));
            }
            if(delta>maxDelta) {
                maxDelta = delta;
            }
        }
        //得到的key可以直接整体放入低载节点
        int sum=0;
        for(Map.Entry<Integer,Integer> entry:taskOverLoadDetail.get(taskIndex).entrySet()){
            int key=entry.getKey();
            int load=entry.getValue();
            if(load<=maxDelta) {
                sum+=load;
                Pair<Integer,Integer> pair=new Pair<Integer, Integer>(key,load);
                keysAsTotal.add(pair);
            }
        }
        if(sum<taskLoad-BL) {
            if(keysAsTotal.size()==0)
                return taskLoad;
            taskLoad=processLittleKeys(keysAsTotal,taskIndex,taskLoad);
            return taskLoad;
        }
        Collections.sort(keysAsTotal, (integerIntegerPair, t1) -> t1.getValue()-integerIntegerPair.getValue());
        keysAsTotal.add(0, new Pair<>(0, 0));
        candidateKeys.clear();
        flag=false;
        int tmp=taskLoad-BL;
        search(tmp,keysAsTotal,sum,true);
        if(candidateKeys.size()==0){
            //System.out.println("大小无误差，无");
            keysAsTotal.remove(0);
            Collections.sort(keysAsTotal, (integerIntegerPair, t1) -> integerIntegerPair.getValue()-t1.getValue());
            keysAsTotal.add(0, new Pair<>(0, 0));
            flag=false;
            search(tmp,keysAsTotal,sum,true);
        }
        if(candidateKeys.size()==0){
            //System.out.println("小大无误差，无");
            keysAsTotal.remove(0);
            Collections.sort(keysAsTotal, (integerIntegerPair, t1) -> t1.getValue()-integerIntegerPair.getValue());
            keysAsTotal.add(0, new Pair<>(0, 0));
            flag=false;
            search(tmp,keysAsTotal,sum,false);
        }
        if(candidateKeys.size()==0){
            //System.out.println("大小有误差，无");
            keysAsTotal.remove(0);
            Collections.sort(keysAsTotal, (integerIntegerPair, t1) -> integerIntegerPair.getValue()-t1.getValue());
            keysAsTotal.add(0, new Pair<>(0, 0));
            flag=false;
            search(tmp,keysAsTotal,sum,false);
        }
        if(candidateKeys.size()>0){
            //System.out.println("小大有误差，有");
            int count=0;
            for(Pair<Integer,Integer> pair:candidateKeys){
                count+=pair.getValue();
            }
            System.out.println("task = "+taskIndex+" , count = "+count+"/"+(tmp));
            List<Map.Entry<Integer,Integer>> boltLoadSortList=getSortedKeys(boltLoad,true);
            int taskCount=0;
            for(Map.Entry<Integer,Integer> taskEntry:boltLoadSortList){
                if(candidateKeys.size()<1)
                    break;
                results.clear();
                resultItem.clear();
                int task=taskEntry.getKey();
                int load=taskEntry.getValue();
                taskCount++;
                if(taskCount==boltLoadSortList.size()){
                    for(Pair<Integer,Integer> keyPair:candidateKeys){
                        int key=keyPair.getKey();
                        int keyLoad=keyPair.getValue();
                        taskLoadDetail.get(task).put(key,keyLoad);
                        taskOverLoadDetail.get(taskIndex).remove(key);
                        addToRouteTable(key,keyLoad,task);
                        if(endCheckInfo.containsKey(task)){
                            endCheckInfo.put(task,endCheckInfo.get(task)+1);
                        }else{
                            endCheckInfo.put(task,1);
                        }
                        if(migrationPlan.containsKey(taskIndex)){
                            migrationPlan.get(taskIndex).add(new MigrationItem(task,key,keyLoad));
                        }else{
                            ArrayList<MigrationItem> l=new ArrayList<>();
                            l.add(new MigrationItem(task,key,keyLoad));
                            migrationPlan.put(taskIndex,l);
                        }
                        taskLoad-=keyLoad;
                        load+=keyLoad;
                    }
                    candidateKeys.clear();
                    boltLoad.put(task,load);
                    break;
                }
                if(load<BL)
                    getKeysWithDefinedSum(candidateKeys,candidateKeys.size()-1,BL-load,upOrDown);
                else
                    continue;
                if(results.size()>0){
                    Collections.sort(results, (pairs, t1) -> t1.size()-pairs.size());
                    SelectList sl=getNearestList(BL-load);
                    boltLoad.put(task,boltLoad.get(task)+sl.getSummary());
                    taskLoad-=sl.getSummary();
                    List<Pair<Integer,Integer>> resultList=results.get(sl.getIndex());
                    for(Pair<Integer,Integer> keyPair:resultList){
                        int key=keyPair.getKey();
                        int keyLoad=keyPair.getValue();
                        taskLoadDetail.get(task).put(key,keyLoad);
                        taskOverLoadDetail.get(taskIndex).remove(key);
                        addToRouteTable(key,keyLoad,task);
                        if(endCheckInfo.containsKey(task)){
                            endCheckInfo.put(task,endCheckInfo.get(task)+1);
                        }else{
                            endCheckInfo.put(task,1);
                        }
                        if(migrationPlan.containsKey(taskIndex)){
                            migrationPlan.get(taskIndex).add(new MigrationItem(task,key,keyLoad));
                        }else{
                            ArrayList<MigrationItem> l=new ArrayList<>();
                            l.add(new MigrationItem(task,key,keyLoad));
                            migrationPlan.put(taskIndex,l);
                        }
                        for(int i=0;i<candidateKeys.size();i++){
                            if(candidateKeys.get(i).getKey()==key) {
                                candidateKeys.remove(i);
                                break;
                            }
                        }
                    }
                }else{
                    Iterator<Pair<Integer,Integer>> listIter=candidateKeys.listIterator();
                    while(load-BL<upOrDown&&listIter.hasNext()){
                        Pair<Integer,Integer> keyPair=listIter.next();
                        int key=keyPair.getKey();
                        int keyLoad=keyPair.getValue();
                        if(load+keyLoad-BL<=upOrDown){
                            taskLoad-=keyLoad;
                            load+=keyLoad;
                            listIter.remove();
                            taskLoadDetail.get(task).put(key,keyLoad);
                            taskOverLoadDetail.get(taskIndex).remove(key);
                            addToRouteTable(key,keyLoad,task);
                            if(endCheckInfo.containsKey(task)){
                                endCheckInfo.put(task,endCheckInfo.get(task)+1);
                            }else{
                                endCheckInfo.put(task,1);
                            }
                            if(migrationPlan.containsKey(taskIndex)){
                                migrationPlan.get(taskIndex).add(new MigrationItem(task,key,keyLoad));
                            }else{
                                ArrayList<MigrationItem> l=new ArrayList<>();
                                l.add(new MigrationItem(task,key,keyLoad));
                                migrationPlan.put(taskIndex,l);
                            }
                        }
                    }
                    boltLoad.put(task,load);
                }
            }
            if(candidateKeys.size()>0){
                for(Pair<Integer,Integer> pair:candidateKeys){
                    System.out.println("key = "+pair.getKey()+" , count = "+pair.getValue()+" 没有安置！");
                }
            }

        }else{
            System.out.println("task ="+taskIndex+"　号没有取出整数的key");
        }
        boltOverLoad.put(taskIndex,taskLoad);
        resultItem.clear();
        results.clear();
        return taskLoad;
    }

    public static SelectList getNearestList(int delta){
        int size=results.get(0).size();
        int minDelta=BL;
        int destIndex=0;
        int destSummary=0;
        for(int i=0;i<results.size();i++){
            if(results.get(i).size()!=size)
                break;
            int summary=getSum(results.get(i));
            if(Math.abs(summary-delta)<minDelta){
                minDelta=Math.abs(summary-delta);
                destIndex=i;
                destSummary=summary;
            }
        }
        return new SelectList(destIndex,destSummary);
    }

    public static int getSum(List<Pair<Integer,Integer>> list){
        int result=0;
        for(Pair<Integer,Integer> pair:list){
            result+=pair.getValue();
        }
        return result;
    }

    public static void processKeysToSplit(int taskIndex,int taskLoad) {
        //临时存储
        HashMap<Integer, RouteTableItem> C = new HashMap<>();
        //先拆分在路由表中的key
        List<Map.Entry<Integer, Integer>> keys = getSortedKeys(keyDetailsInTable,false);
        Iterator<Map.Entry<Integer, Integer>> keyInTableIter = keys.iterator();
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
                        //System.out.println();
                    }
                }
                keyDetailsEntry.setValue(load - delta);
                keyDetailsInTable.put(key, load - delta);
            }
        }
        //再拆分不在路由表中的key
        keys = getSortedKeys(taskOverLoadDetail.get(taskIndex),false);
        Iterator<Map.Entry<Integer, Integer>> keyNotInTableIter = keys.iterator();
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
                if(endCheckInfo.containsKey(lowIndex)){
                    endCheckInfo.put(lowIndex,endCheckInfo.get(lowIndex)+1);
                }else{
                    endCheckInfo.put(lowIndex,1);
                }
                if(migrationPlan.containsKey(taskIndex)){
                    migrationPlan.get(taskIndex).add(new MigrationItem(lowIndex,key,temp));
                }else{
                    ArrayList<MigrationItem> l=new ArrayList<>();
                    l.add(new MigrationItem(lowIndex,key,temp));
                    migrationPlan.put(taskIndex,l);
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
        keyDetailsInTable.clear();
        endCheckInfo.clear();
        resultItem.clear();
        results.clear();
        migrationPlan.clear();
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

    public static void testMNRT(){
        loadDataFromSingleFile("DataSource/keys_48w_150_0_5.txt");
        writeLoadToFile("","MNRT");
        makeplanMNRT();
        writeTableToFile("DataSource/Test/MNRT/tableNotSplit.txt");
        writeLoadToFile("_after","MNRT");
        for(Map.Entry<Integer,ArrayList<MigrationItem>> mpi:migrationPlan.entrySet()){
            int task=mpi.getKey();
            ArrayList<MigrationItem> list=mpi.getValue();
            System.out.println("从"+task+"号节点往外迁移:");
            for(MigrationItem mi:list){
                System.out.println("迁往"+mi.getTo()+", key = "+mi.getKey()+", count = "+mi.getCount());
            }
            System.out.println();
        }
        System.out.println("endcheckinfo");
        for(Map.Entry<Integer,Integer> entry:endCheckInfo.entrySet()){
            System.out.println("task = "+entry.getKey()+" , number = "+entry.getValue());
        }
    }


    public static List<Map.Entry<Integer, Integer>> getSortedKeys(HashMap<Integer,Integer> keys,boolean flag){
        ArrayList<Integer> list=new ArrayList<>();
        List<Map.Entry<Integer, Integer>> values = new ArrayList<>(keys.entrySet());
        if(flag==true){
            //由大到小排列
            Collections.sort(values, (t1, t2) -> t2.getValue().compareTo(t1.getValue()));
        }else{
            //由小到大排列
            Collections.sort(values, (t1, t2) -> t1.getValue().compareTo(t2.getValue()));
        }
        return values;
    }



    public static void getKeysWithDefinedSum(List<Pair<Integer,Integer>>array, int i,int sum,int para){
        if(sum<=0||i<0)
            return;
        if(Math.abs(sum-array.get(i).getValue())<=para){
            flag=true;
            //if(sum==array.get(i).getValue()){
            ArrayList<Pair<Integer,Integer>> temp=new ArrayList<>(resultItem);
            temp.add(array.get(i));
            results.add(temp);
            return;
        }
        resultItem.push(array.get(i));
        getKeysWithDefinedSum(array,i-1,sum-array.get(i).getValue(),para);
        resultItem.pop();
        getKeysWithDefinedSum(array,i-1,sum,para);
    }

    public static Boolean flag=false;
    public static Boolean[] X;
    public static Integer r;
    public static List<Pair<Integer,Integer>> candidateKeys=new ArrayList<>();

    public static void sumOfKNumber2(int t,int k,int r,Integer M, Boolean[] X,List<Pair<Integer,Integer>> N){
        if(flag==true)
            return;
        X[k]=true;
        if(Math.abs(t+N.get(k).getValue()-M)<=20){
        //if(t+N.get(k).getValue()==M){
            flag=true;
            for(int i=1;i<=k;i++){
                if(X[i]==true){
                    System.out.print(N.get(i).getValue()+" ");
                    candidateKeys.add(N.get(i));
                }
            }
            //System.out.println();
        }else{
            if(t+N.get(k).getValue()+N.get(k+1).getValue()<=M){
                sumOfKNumber2(t+N.get(k).getValue(),k+1,r-N.get(k).getValue(),M,X,N);
            }
            if((t+r-N.get(k).getValue()>=M)&&(t+N.get(k+1).getValue())<=M){
                X[k]=false;
                sumOfKNumber2(t,k+1,r-N.get(k).getValue(),M,X,N);
            }
        }
    }

    public static void sumOfKNumber(int t,int k,int r,Integer M, Boolean[] X,List<Pair<Integer,Integer>> N){
        if(flag==true)
            return;
        X[k]=true;
        //if(Math.abs(t+N.get(k).getValue()-M)<=20){
        if(t+N.get(k).getValue()==M){
            flag=true;
            for(int i=1;i<=k;i++){
                if(X[i]==true){
                    System.out.print(N.get(i).getValue()+" ");
                    candidateKeys.add(N.get(i));
                }
            }
            //System.out.println();
        }else{
            if(t+N.get(k).getValue()+N.get(k+1).getValue()<=M){
                sumOfKNumber(t+N.get(k).getValue(),k+1,r-N.get(k).getValue(),M,X,N);
            }
            if((t+r-N.get(k).getValue()>=M)&&(t+N.get(k+1).getValue())<=M){
                X[k]=false;
                sumOfKNumber(t,k+1,r-N.get(k).getValue(),M,X,N);
            }
        }
    }

    public static void search(Integer M,List<Pair<Integer,Integer>> N,int sum,boolean FLAG){
        X=new Boolean[N.size()+1];
        r=sum;
        flag=false;
        for(int i=0;i<N.size()+1;i++){
            X[i]=false;
        }
        if(FLAG==true)
            sumOfKNumber(0,1,r,M,X,N);
        else
            sumOfKNumber2(0,1,r,M,X,N);
    }

    public static void main(String[] args) {
        testMNRT();

    }
}

package util.mfm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by stream on 16-9-26.
 */
public class MigrateUtility implements Serializable,Comparable<MigrateUtility>{
    private int mc;
    private int load;

    public MigrateUtility(int mc,int load){
        this.mc=mc;
        this.load=load;
    }

    public int getMc() {
        return mc;
    }

    public void setMc(int mc) {
        this.mc = mc;
    }

    public int getLoad() {
        return load;
    }

    public void setLoad(int load) {
        this.load = load;
    }

    public String toString(){
        return "mc = "+mc+" , load = "+load;
    }


    @Override
    public int compareTo(MigrateUtility migrateUtility) {
        Double ratio_this=new Double((double)mc/load);
        Double ratio_other=new Double((double)migrateUtility.getMc()/migrateUtility.getLoad());
        return ratio_this.compareTo(ratio_other);
    }

    public static void main(String[] args){
        List<MigrateUtility> list=new ArrayList<>();
        list.add(new MigrateUtility(1,24));
        list.add(new MigrateUtility(23,24));
        list.add(new MigrateUtility(2,24));
        Collections.sort(list);
        for(MigrateUtility item:list){
            System.out.println(item.toString());
        }
    }
}

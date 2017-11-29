package util.loadbalance;

import java.io.Serializable;

/**
 * Created by stream on 16-10-11.
 */
public class MigrationItem implements Serializable{

    private int to;
    private int key;
    private int count;

    public MigrationItem(int to,int key,int count){
        this.count=count;
        this.to=to;
        this.key=key;
    }

    public int getTo() {
        return to;
    }

    public void setTo(int to) {
        this.to = to;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String toString(){
        return "[ moveto = "+ to+" , key = "+key+" , count = "+count+" ]";
    }
}

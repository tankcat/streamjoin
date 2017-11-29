package util;

import java.io.Serializable;

/**
 * Created by stream on 16-9-13.
 */
public class RouteTableItem implements Serializable,Comparable<RouteTableItem>{
    private int count;
    private int taskIndex;
    public RouteTableItem(int count,int taskIndex){
        this.count=count;
        this.taskIndex=taskIndex;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getTaskIndex() {
        return taskIndex;
    }

    public void setTaskIndex(int taskIndex) {
        this.taskIndex = taskIndex;
    }

    @Override
    public int compareTo(RouteTableItem routeTableItem) {
        return this.count-routeTableItem.count;
    }

    public String toString(){
        return " [taskIndex = "+taskIndex+", count = "+count+"] ";
    }
}

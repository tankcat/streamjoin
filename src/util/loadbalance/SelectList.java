package util.loadbalance;

/**
 * Created by stream on 16-10-15.
 */
public class SelectList {
    private int index;
    private int summary;

    public SelectList(int index, int summary) {
        this.index = index;
        this.summary = summary;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getSummary() {
        return summary;
    }

    public void setSummary(int summary) {
        this.summary = summary;
    }
}

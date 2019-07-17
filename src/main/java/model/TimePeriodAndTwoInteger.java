package model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimePeriodAndTwoInteger extends TimePeriodAndSomething<TimePeriodAndTwoInteger, TwoInteger> {
    private TwoInteger item;

    public TimePeriodAndTwoInteger(Integer period, TwoInteger item) {
        super(period, item);
    }

    public TimePeriodAndTwoInteger(){
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(period);
        out.writeInt(item.getA1());
        out.writeInt(item.getA2());
    }

    public void readFields(DataInput in) throws IOException {
        period = in.readInt();
        item.setA1(in.readInt());
        item.setA2(in.readInt());
    }

    @Override
    public String toString() {
        return period + "\t" + item;
    }

    public int compareTo(TimePeriodAndTwoInteger o) {
        if (period.compareTo(o.getPeriod()) == 0) {
            return item.compareTo(o.getItem());
        }
        return period.compareTo(o.getPeriod());
    }
}

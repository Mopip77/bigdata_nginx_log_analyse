package model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimePeriodAndThreeInteger extends TimePeriodAndSomething<TimePeriodAndThreeInteger, ThreeInteger> {

    public TimePeriodAndThreeInteger(Integer period, ThreeInteger item) {
        super(period, item);
    }

    public TimePeriodAndThreeInteger(){
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(period);
        item.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        period = in.readInt();
        setItem(new ThreeInteger());
        item.readFields(in);
    }

    @Override
    public String toString() {
        return period + "\t" + item;
    }

    public int compareTo(TimePeriodAndThreeInteger o) {
        if (period.compareTo(o.getPeriod()) == 0) {
            return item.compareTo(o.getItem());
        }
        return period.compareTo(o.getPeriod());
    }
}

package model;

import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimePeriodAndText extends TimePeriodAndSomething<TimePeriodAndText, Text> {

    public TimePeriodAndText(Integer period, Text item) {
        super(period, item);
    }

    public TimePeriodAndText(){

    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(period);
        out.writeUTF(item.toString());
    }

    public void readFields(DataInput in) throws IOException {
        period = in.readInt();
        item = new Text(in.readUTF());
    }

    @Override
    public String toString() {
        return period + "\t" + item;
    }

    public int compareTo(TimePeriodAndText o) {
        if (period.compareTo(o.getPeriod()) == 0) {
            return item.compareTo(o.getItem());
        }
        return period.compareTo(o.getPeriod());
    }
}

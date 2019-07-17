package model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TwoInteger implements WritableComparable<TwoInteger> {

    private Integer a1;
    private Integer a2;

    public TwoInteger(Integer a1, Integer a2) {
        this.a1 = a1;
        this.a2 = a2;
    }

    public TwoInteger() {
    }

    public int compareTo(TwoInteger o) {
        int compare = a1.compareTo(o.getA1());
        if (compare != 0)
            return compare;

        return a2.compareTo(o.getA2());
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(a1);
        out.writeInt(a2);
    }

    public void readFields(DataInput in) throws IOException {
        a1 = in.readInt();
        a2 = in.readInt();
    }

    @Override
    public String toString() {
        return a1 + "\t" + a2;
    }

    public Integer getA1() {
        return a1;
    }

    public void setA1(Integer a1) {
        this.a1 = a1;
    }

    public Integer getA2() {
        return a2;
    }

    public void setA2(Integer a2) {
        this.a2 = a2;
    }
}
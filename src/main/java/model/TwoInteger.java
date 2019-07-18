package model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TwoInteger implements WritableComparable<TwoInteger> {

    private static final String className = TwoInteger.class.getName();

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

    public static int compare(TwoInteger o1, TwoInteger o2, boolean inverse, int mainkey) {
        Integer a1 = o1.getA1(); Integer a2 = o1.getA2();
        int compare = 0;
        if (mainkey == 1) {
            compare = a2.compareTo(o2.getA2());
        }

        if (compare == 0) {
            compare = a1.compareTo(o2.getA1());
            if (compare == 0) {
                compare = a2.compareTo(o2.getA2());
            }
        }

        return (inverse ? -1 : 1) * compare;
    }

    private static class Comparator extends WritableComparator {
        public Comparator() {
            super(TwoInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TwoInteger a1 = (TwoInteger) a;
            TwoInteger b1 = (TwoInteger) b;
            return TwoInteger.compare(a1, b1, false, 0);
        }
    }

    private static class ReverseComparator extends WritableComparator {
        public ReverseComparator() {
            super(TwoInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TwoInteger a1 = (TwoInteger) a;
            TwoInteger b1 = (TwoInteger) b;
            return TwoInteger.compare(a1, b1, true, 0);
        }
    }

    private static class Mainkey1Comparator extends WritableComparator {
        public Mainkey1Comparator() {
            super(TwoInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TwoInteger a1 = (TwoInteger) a;
            TwoInteger b1 = (TwoInteger) b;
            return TwoInteger.compare(a1, b1, false, 1);
        }
    }

    private static class Mainkey1ReverseComparator extends WritableComparator {
        public Mainkey1ReverseComparator() {
            super(TwoInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TwoInteger a1 = (TwoInteger) a;
            TwoInteger b1 = (TwoInteger) b;
            return TwoInteger.compare(a1, b1, true, 1);
        }
    }

    public static Class getComparator(boolean reverse, int mainkey) {
        String comparatorClassName;
        if (mainkey == 0) {
            comparatorClassName = className + "$" + (reverse ? "Reverse" : "") + "Comparator";
        } else {

            comparatorClassName = className + "$" + "Mainkey" + mainkey + (reverse ? "Reverse" : "") + "Comparator";
        }
        try {
            return Class.forName(comparatorClassName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
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

package model;

import comparator.SomeThingOfDateAndSomeThingComaratorFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ThreeInteger implements WritableComparable<ThreeInteger> {

    private static String className = ThreeInteger.class.getName();

    private Integer a1;
    private Integer a2;
    private Integer a3;

    public ThreeInteger(Integer a1, Integer a2, Integer a3) {
        this.a1 = a1;
        this.a2 = a2;
        this.a3 = a3;
    }

    public ThreeInteger() {
    }

    public int compareTo(ThreeInteger o) {
        int compare = a1.compareTo(o.getA1());
        if (compare != 0)
            return compare;

        compare = a2.compareTo(o.getA2());
        if (compare != 0)
            return compare;

        return a3.compareTo(o.getA3());
    }

    public static int compare(ThreeInteger o1, ThreeInteger o2, boolean inverse, int mainKey) {
        Integer a1 = o1.getA1(); Integer a2 = o1.getA2(); Integer a3 = o1.getA3();
        int compare = 0;
        if (mainKey == 1) {
            compare = a2.compareTo(o2.getA2());
        } else if (mainKey == 2) {
            compare = a2.compareTo(o2.getA2());
        }

        if (compare == 0) {
            compare = a1.compareTo(o2.getA1());
            if (compare == 0) {
                compare = a2.compareTo(o2.getA2());
                if (compare == 0) {
                    compare = a3.compareTo(o2.getA3());

                }
            }
        }

        return (inverse ? -1 : 1) * compare;
    }

    private static class Comparator extends WritableComparator {
        public Comparator() {
            super(ThreeInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ThreeInteger a1 = (ThreeInteger) a;
            ThreeInteger b1 = (ThreeInteger) b;
            return ThreeInteger.compare(a1, b1, false, 0);
        }
    }

    private static class ReverseComparator extends WritableComparator {
        public ReverseComparator() {
            super(ThreeInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ThreeInteger a1 = (ThreeInteger) a;
            ThreeInteger b1 = (ThreeInteger) b;
            return ThreeInteger.compare(a1, b1, true, 0);
        }
    }

    private static class Mainkey1Comparator extends WritableComparator {
        public Mainkey1Comparator() {
            super(ThreeInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ThreeInteger a1 = (ThreeInteger) a;
            ThreeInteger b1 = (ThreeInteger) b;
            return ThreeInteger.compare(a1, b1, false, 1);
        }
    }

    private static class Mainkey1ReverseComparator extends WritableComparator {
        public Mainkey1ReverseComparator() {
            super(ThreeInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ThreeInteger a1 = (ThreeInteger) a;
            ThreeInteger b1 = (ThreeInteger) b;
            return ThreeInteger.compare(a1, b1, true, 1);
        }
    }

    private static class Mainkey2Comparator extends WritableComparator {
        public Mainkey2Comparator() {
            super(ThreeInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ThreeInteger a1 = (ThreeInteger) a;
            ThreeInteger b1 = (ThreeInteger) b;
            return ThreeInteger.compare(a1, b1, false, 2);
        }
    }

    private static class Mainkey2ReverseComparator extends WritableComparator {
        public Mainkey2ReverseComparator() {
            super(ThreeInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            ThreeInteger a1 = (ThreeInteger) a;
            ThreeInteger b1 = (ThreeInteger) b;
            return ThreeInteger.compare(a1, b1, true, 2);
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
        out.writeInt(a3);
    }

    public void readFields(DataInput in) throws IOException {
        a1 = in.readInt();
        a2 = in.readInt();
        a3 = in.readInt();
    }

    @Override
    public String toString() {
        return a1 + "\t" + a2 + "\t" + a3;
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

    public Integer getA3() {
        return a3;
    }

    public void setA3(Integer a3) {
        this.a3 = a3;
    }
}

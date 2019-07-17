package comparator;

import model.TwoInteger;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TwoIntegerComparatorFactory {

    private static String className = TwoIntegerComparatorFactory.class.getName();

    private static class TwoIntegerComparator extends WritableComparator {
        public TwoIntegerComparator() {
            super(TwoInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TwoInteger a1 = (TwoInteger) a;
            TwoInteger b1 = (TwoInteger) b;
            return TwoDimensionComarator.compare(a1.getA1(), a1.getA2(), b1.getA1(), b1.getA2(), false, false);
        }
    }

    private static class TwoIntegerReverseComparator extends WritableComparator {
        public TwoIntegerReverseComparator() {
            super(TwoInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TwoInteger a1 = (TwoInteger) a;
            TwoInteger b1 = (TwoInteger) b;
            return TwoDimensionComarator.compare(a1.getA1(), a1.getA2(), b1.getA1(), b1.getA2(), true, false);
        }
    }

    private static class TwoIntegerSubkeyComparator extends WritableComparator {
        public TwoIntegerSubkeyComparator() {
            super(TwoInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TwoInteger a1 = (TwoInteger) a;
            TwoInteger b1 = (TwoInteger) b;
            return TwoDimensionComarator.compare(a1.getA1(), a1.getA2(), b1.getA1(), b1.getA2(), false, true);
        }
    }

    private static class TwoIntegerSubkeyReverseComparator extends WritableComparator {
        public TwoIntegerSubkeyReverseComparator() {
            super(TwoInteger.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TwoInteger a1 = (TwoInteger) a;
            TwoInteger b1 = (TwoInteger) b;
            return TwoDimensionComarator.compare(a1.getA1(), a1.getA2(), b1.getA1(), b1.getA2(), true, true);
        }
    }

    public static Class getComparator(boolean inverserCompare, boolean isSubkey) {
        String comparatorClassName = className + "$" + "TwoInteger" + (isSubkey ? "Subkey" : "") + (inverserCompare ? "Reverse" : "") + "Comparator";
        try {
            return Class.forName(comparatorClassName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}

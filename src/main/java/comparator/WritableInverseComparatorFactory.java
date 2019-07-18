package comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WritableInverseComparatorFactory {

    private static final String className = WritableInverseComparatorFactory.class.getName();
    private static class IntWritableReverseComparator extends WritableComparator {
        public IntWritableReverseComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    public static Class getComparator(Class writeComparableClass) {
        String comparatorClassName = className + "$" + writeComparableClass.getSimpleName() + "ReverseComparator";
        try {
            return Class.forName(comparatorClassName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }
}

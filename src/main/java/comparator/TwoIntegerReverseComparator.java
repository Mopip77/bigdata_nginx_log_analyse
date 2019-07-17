package comparator;

import model.TwoInteger;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TwoIntegerReverseComparator extends WritableComparator {
    public TwoIntegerReverseComparator() {
        super(TwoInteger.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TwoInteger a1 = (TwoInteger) a;
        TwoInteger b1 = (TwoInteger) b;
        return -a1.compareTo(b1);
    }
}

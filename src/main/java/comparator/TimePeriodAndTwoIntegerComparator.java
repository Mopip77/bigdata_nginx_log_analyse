package comparator;

import model.TimePeriodAndTwoInteger;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TimePeriodAndTwoIntegerComparator extends WritableComparator {
    public TimePeriodAndTwoIntegerComparator() {
        super(TimePeriodAndTwoInteger.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
//        return 0;
        TimePeriodAndTwoInteger a1 = (TimePeriodAndTwoInteger) a;
        TimePeriodAndTwoInteger b1 = (TimePeriodAndTwoInteger) b;
        int firstCompare = a1.getPeriod().compareTo(b1.getPeriod());
        return firstCompare != 0 ? firstCompare : a1.getItem().compareTo(b1.getItem());
    }
}

package comparator;

import model.TimePeriodAndSomething;
import model.TimePeriodAndTwoInteger;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TimePeriodAndSomeThingComparator extends WritableComparator {
    public TimePeriodAndSomeThingComparator() {
        super(TimePeriodAndTwoInteger.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
//        return 0;
        TimePeriodAndSomething a1 = (TimePeriodAndSomething) a;
        TimePeriodAndSomething b1 = (TimePeriodAndSomething) b;
        int firstCompare = a1.getPeriod().compareTo(b1.getPeriod());
        return firstCompare != 0 ? firstCompare : a1.getItem().compareTo(b1.getItem());
    }
}

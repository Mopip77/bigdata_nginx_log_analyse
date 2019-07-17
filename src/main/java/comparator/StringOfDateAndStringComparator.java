package comparator;

import model.TimePeriodAndSomething;
import model.TimePeriodAndText;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StringOfDateAndStringComparator extends WritableComparator {
    public StringOfDateAndStringComparator() {
        super(TimePeriodAndSomething.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
//        return 0;
        TimePeriodAndText a1 = (TimePeriodAndText) a;
        TimePeriodAndText b1 = (TimePeriodAndText) b;
        return a1.getItem().compareTo(b1.getItem());
//        int firstCompare = a1.getDate().compareTo(b1.getDate());
//        return firstCompare != 0 ? firstCompare : a1.getItem().compareTo(b1.getItem());
    }
}

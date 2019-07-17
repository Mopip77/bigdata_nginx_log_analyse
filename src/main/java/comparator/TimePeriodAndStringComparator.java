package comparator;

import model.TimePeriodAndText;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TimePeriodAndStringComparator extends WritableComparator {
    public TimePeriodAndStringComparator() {
        super(TimePeriodAndText.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
//        return 0;
        TimePeriodAndText a1 = (TimePeriodAndText) a;
        TimePeriodAndText b1 = (TimePeriodAndText) b;
        int firstCompare = a1.getPeriod().compareTo(b1.getPeriod());
        return firstCompare != 0 ? firstCompare : a1.getItem().compareTo(b1.getItem());
    }
}

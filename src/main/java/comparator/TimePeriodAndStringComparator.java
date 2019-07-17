package comparator;

import model.DateAndString;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateAndStringComparator extends WritableComparator {
    public DateAndStringComparator() {
        super(DateAndString.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
//        return 0;
        DateAndString a1 = (DateAndString) a;
        DateAndString b1 = (DateAndString) b;
        int firstCompare = a1.getDate().compareTo(b1.getDate());
        return firstCompare != 0 ? firstCompare : a1.getItem().compareTo(b1.getItem());
    }
}

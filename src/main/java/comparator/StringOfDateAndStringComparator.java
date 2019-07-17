package comparator;

import model.DateAndString;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StringOfDateAndStringComparator extends WritableComparator {
    public StringOfDateAndStringComparator() {
        super(DateAndString.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
//        return 0;
        DateAndString a1 = (DateAndString) a;
        DateAndString b1 = (DateAndString) b;
        return a1.getItem().compareTo(b1.getItem());
//        int firstCompare = a1.getDate().compareTo(b1.getDate());
//        return firstCompare != 0 ? firstCompare : a1.getItem().compareTo(b1.getItem());
    }
}

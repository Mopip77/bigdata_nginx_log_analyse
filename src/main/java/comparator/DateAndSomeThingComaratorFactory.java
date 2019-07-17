package comparator;

import model.TimePeriodAndSomething;
import model.TimePeriodAndText;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import sun.util.cldr.CLDRLocaleDataMetaInfo;

public class DateAndSomeThingComaratorFactory {

    static class BaseComparator<T extends TimePeriodAndSomething> extends WritableComparator {
        public BaseComparator(Class model) {
            super(model, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            T a1 = (T) a;
            T b1 = (T) b;
            return a1.getItem().compareTo(b1.getItem());
        }
    }

    static class TextComparator extends BaseComparator<TimePeriodAndText> {
        public TextComparator() {
            super(TimePeriodAndText.class);
        }
    }

    public static Class getComparator(Class model) {
        return new BaseComparator(model).getClass();
    }

    public static Class tete() {
        return TextComparator.class;
    }
}

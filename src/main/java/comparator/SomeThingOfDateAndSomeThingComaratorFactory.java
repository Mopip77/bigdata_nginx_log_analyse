package comparator;

import model.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SomeThingOfDateAndSomeThingComaratorFactory {

    private static String className = SomeThingOfDateAndSomeThingComaratorFactory.class.getName();

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

    private static class TextComparator extends BaseComparator<TimePeriodAndText> {
        public TextComparator() {
            super(TimePeriodAndText.class);
        }
    }

    private static class TextReverseComparator extends BaseComparator<TimePeriodAndText> {
        public TextReverseComparator() {
            super(TimePeriodAndText.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
    }

    private static class TwoIntegerComparator extends BaseComparator<TimePeriodAndTwoInteger> {
        public TwoIntegerComparator() {
            super(TimePeriodAndTwoInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndTwoInteger a1 = (TimePeriodAndTwoInteger) a;
            TimePeriodAndTwoInteger b1 = (TimePeriodAndTwoInteger) b;
            return TwoInteger.compare(a1.getItem(), b1.getItem(), false, 0);
        }
    }

    private static class TwoIntegerReverseComparator extends BaseComparator<TimePeriodAndTwoInteger> {
        public TwoIntegerReverseComparator() {
            super(TimePeriodAndTwoInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndTwoInteger a1 = (TimePeriodAndTwoInteger) a;
            TimePeriodAndTwoInteger b1 = (TimePeriodAndTwoInteger) b;
            return TwoInteger.compare(a1.getItem(), b1.getItem(), true, 0);
        }
    }

    private static class TwoIntegerMainkey1Comparator extends BaseComparator<TimePeriodAndTwoInteger> {
        public TwoIntegerMainkey1Comparator() {
            super(TimePeriodAndTwoInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndTwoInteger a1 = (TimePeriodAndTwoInteger) a;
            TimePeriodAndTwoInteger b1 = (TimePeriodAndTwoInteger) b;
            return TwoInteger.compare(a1.getItem(), b1.getItem(), false, 1);
        }
    }

    private static class TwoIntegerMainkey1ReverseComparator extends BaseComparator<TimePeriodAndTwoInteger> {
        public TwoIntegerMainkey1ReverseComparator() {
            super(TimePeriodAndTwoInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndTwoInteger a1 = (TimePeriodAndTwoInteger) a;
            TimePeriodAndTwoInteger b1 = (TimePeriodAndTwoInteger) b;
            return TwoInteger.compare(a1.getItem(), b1.getItem(), true, 1);
        }
    }

    private static class ThreeIntegerComparator extends BaseComparator<TimePeriodAndThreeInteger> {
        public ThreeIntegerComparator() {
            super(TimePeriodAndThreeInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndThreeInteger a1 = (TimePeriodAndThreeInteger) a;
            TimePeriodAndThreeInteger b1 = (TimePeriodAndThreeInteger) b;
            return ThreeInteger.compare(a1.getItem(), b1.getItem(), false, 0);
        }
    }

    private static class ThreeIntegerReverseComparator extends BaseComparator<TimePeriodAndThreeInteger> {
        public ThreeIntegerReverseComparator() {
            super(TimePeriodAndThreeInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndThreeInteger a1 = (TimePeriodAndThreeInteger) a;
            TimePeriodAndThreeInteger b1 = (TimePeriodAndThreeInteger) b;
            return ThreeInteger.compare(a1.getItem(), b1.getItem(), true, 0);
        }
    }

    private static class ThreeIntegerMainkey1Comparator extends BaseComparator<TimePeriodAndThreeInteger> {
        public ThreeIntegerMainkey1Comparator() {
            super(TimePeriodAndThreeInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndThreeInteger a1 = (TimePeriodAndThreeInteger) a;
            TimePeriodAndThreeInteger b1 = (TimePeriodAndThreeInteger) b;
            return ThreeInteger.compare(a1.getItem(), b1.getItem(), false, 1);
        }
    }

    private static class ThreeIntegerMainkey1ReverseComparator extends BaseComparator<TimePeriodAndThreeInteger> {
        public ThreeIntegerMainkey1ReverseComparator() {
            super(TimePeriodAndThreeInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndThreeInteger a1 = (TimePeriodAndThreeInteger) a;
            TimePeriodAndThreeInteger b1 = (TimePeriodAndThreeInteger) b;
            return ThreeInteger.compare(a1.getItem(), b1.getItem(), true, 1);
        }
    }

    private static class ThreeIntegerMainkey2Comparator extends BaseComparator<TimePeriodAndThreeInteger> {
        public ThreeIntegerMainkey2Comparator() {
            super(TimePeriodAndThreeInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndThreeInteger a1 = (TimePeriodAndThreeInteger) a;
            TimePeriodAndThreeInteger b1 = (TimePeriodAndThreeInteger) b;
            return ThreeInteger.compare(a1.getItem(), b1.getItem(), false, 2);
        }
    }

    private static class ThreeIntegerMainkey2ReverseComparator extends BaseComparator<TimePeriodAndThreeInteger> {
        public ThreeIntegerMainkey2ReverseComparator() {
            super(TimePeriodAndThreeInteger.class);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            TimePeriodAndThreeInteger a1 = (TimePeriodAndThreeInteger) a;
            TimePeriodAndThreeInteger b1 = (TimePeriodAndThreeInteger) b;
            return ThreeInteger.compare(a1.getItem(), b1.getItem(), true, 2);
        }
    }

    public static Class getComparator(Class model, boolean inverserCompare, int mainkey) {
        String typeNameOfSomething = null;
        if (model == TimePeriodAndText.class)
            typeNameOfSomething = "Text";
        else if (model == TimePeriodAndTwoInteger.class)
            typeNameOfSomething = "TwoInteger";
        else if (model == TimePeriodAndThreeInteger.class)
            typeNameOfSomething = "ThreeInteger";

        if (typeNameOfSomething == null)
            return null;

        String comparatorClassName;
        // 单值的mainkey无效或者mainkey为0
        if (typeNameOfSomething.equals("Text") || mainkey == 0) {
            comparatorClassName = className + "$" + typeNameOfSomething +  (inverserCompare ? "Reverse" : "") + "Comparator";
        } else {
            comparatorClassName = className + "$" + typeNameOfSomething + "Mainkey" + mainkey + (inverserCompare ? "Reverse" : "") + "Comparator";
        }
        try {
            return Class.forName(comparatorClassName);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }
}

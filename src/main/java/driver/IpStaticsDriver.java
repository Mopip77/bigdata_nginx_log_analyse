package driver;

import comparator.WritableInverseComparatorFactory;
import job.IpStaticsJob;
import job.MyJob;
import job.ValueSortJob;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class IpStaticsDriver {

    private static int tmpFolderNum = 0;

    public static String getTmpFolder(boolean inc) {
        if (inc)
            tmpFolderNum++;
        return "tmp" + tmpFolderNum + "/";
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        MyJob parserJob = new IpStaticsJob();
        parserJob.setup(args[0], getTmpFolder(true), IpStaticsDriver.class);

        ValueSortJob valueSortJob = new ValueSortJob();
        valueSortJob.setup(getTmpFolder(false), args[1], IpStaticsDriver.class, WritableInverseComparatorFactory.class);

        if (parserJob.run()) {
            System.exit(valueSortJob.run() ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}

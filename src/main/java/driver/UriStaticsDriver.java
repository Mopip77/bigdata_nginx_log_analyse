package driver;

import job.MyJob;
import job.UriStaticsJob;
import job.ValueSortJob;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class UriStaticsDriver {

    private static int tmpFolderNum = 0;

    public static String getTmpFolder(boolean inc) {
        if (inc)
            tmpFolderNum++;
        return "tmp" + tmpFolderNum + "/";
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        MyJob parserJob = new UriStaticsJob();
        parserJob.setup(args[0], getTmpFolder(true), UriStaticsDriver.class);

        MyJob valueSortJob = new ValueSortJob();
        valueSortJob.setup(getTmpFolder(false), args[1], UriStaticsDriver.class);

        if (parserJob.run()) {
            System.exit(valueSortJob.run() ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}

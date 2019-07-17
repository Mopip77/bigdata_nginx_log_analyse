package job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import properties.MyProperties;

import java.io.IOException;
import java.lang.reflect.Method;

public abstract class MyJob {

    protected Job job;
    protected Configuration configuration;
    protected static String JOB_NAME;

    public abstract void setup(String srcPath, String OutputPath, Class driver) throws IOException;
    public boolean run() throws InterruptedException, IOException, ClassNotFoundException {
        return job.waitForCompletion(true);
    };
    public Job getJob() {
        return job;
    };

    public static int calNumTasks() {
        long startTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("startTimeTimeStamp"));
        long endTime = Long.valueOf(MyProperties.getInstance().getPro().getProperty("endTimeTimeStamp"));
        int timePeriod = Integer.valueOf(MyProperties.getInstance().getPro().getProperty("timePeriod"));
        int numTasks = 3;
        if (endTime != 0) {
            numTasks = (int) ((endTime - startTime - 1) / timePeriod + 1);
        }
        return numTasks;
    }
}

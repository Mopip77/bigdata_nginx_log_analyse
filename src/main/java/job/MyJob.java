package job;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public interface MyJob {
    void setup(String srcPath, String OutputPath, Class driver) throws IOException;
    boolean run() throws InterruptedException, IOException, ClassNotFoundException;
    Job getJob();
}

package properties;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class MyProperties {
    private Properties pro;
    private FileInputStream in;


    private static MyProperties  ourInstance;

    public static MyProperties getInstance() {
        if (ourInstance == null) {

            try {
                ourInstance = new MyProperties();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return ourInstance;
    }

    private void preProcess() {
        String startTime = pro.getProperty("startTime");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");

        try {
            Date startTimeDate = simpleDateFormat.parse(startTime);
            pro.setProperty("startTimeTimeStamp", String.valueOf(startTimeDate.getTime() / 1000));
        } catch (ParseException e) {
            pro.setProperty("startTimeTimeStamp", "0");
        }

        String endTime = pro.getProperty("endTime");

        try {
            Date endTimeDate = simpleDateFormat.parse(endTime);
            pro.setProperty("endTimeTimeStamp", String.valueOf(endTimeDate.getTime() / 1000));
        } catch (ParseException e) {
            pro.setProperty("endTimeTimeStamp", "0");
        }

        String timePeriod = pro.getProperty("timePeriod");
        pro.setProperty("timePeriod", String.valueOf(Integer.valueOf(timePeriod) * 3600));
    }

    private MyProperties() throws IOException {
        pro = new Properties();
        in = new FileInputStream("src/sys.properties");
        pro.load(in);
        preProcess();
    }

    public Properties getPro() {
        return pro;
    }
}

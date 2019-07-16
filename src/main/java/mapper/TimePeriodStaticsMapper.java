package mapper;//package mapper;
//
//import model.DateAndLogItem;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//import java.io.IOException;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.Locale;
//
//public class TimePeriodStaticsMapper extends Mapper<LongWritable, Text, DateAndLogItem, IntWritable> {
//
//    // 时间间隔 x 小时
//    private static final Integer TIME_PERIOD = 1;
//    private static final Calendar calendar = Calendar.getInstance();
//
//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        String s = value.toString();
//        String[] split = s.split("-");
//        String ip = split[0];
//        String dateString = split[1];
//        String uri = split[2];
//        Integer statusCode = Integer.valueOf(split[3]);
//        Integer flux = Integer.valueOf(split[4].split("\\s+")[0]);
//
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH);
//        try {
//            Date date = simpleDateFormat.parse(dateString);
//            calendar.setTime(date);
//            int hour = calendar.get(Calendar.HOUR);
//            hour -= hour % TIME_PERIOD;
//            calendar.set(Calendar.HOUR, hour);
//            calendar.set(Calendar.MINUTE, 0);
//            calendar.set(Calendar.SECOND, 0);
//            long baseTime = calendar.getTime().getTime();
//            DateAndLogItem dateAndLogItem = new DateAndLogItem(baseTime, ip, dateString, uri, statusCode, flux);
//            context.write(dateAndLogItem, new IntWritable(1));
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//    }
//}

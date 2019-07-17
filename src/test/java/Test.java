import properties.MyProperties;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Test {
    @org.junit.Test
    public void ts() throws ParseException {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH);
        Date parse = simpleDateFormat.parse("30/Mar/2015:17:38:20 +0800");


//        DateTimeFormatter df = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
//        LocalDate parse = LocalDate.parse("30/Mar/2015:17:38:20", df);
        System.out.println(" parses as " + parse);

    }

    @org.junit.Test
    public void ha() {
        String a = "asdf/zxcv/ewf/svxc.fsd";
        String[] split = a.split("/[^/]*$");
        for (String s : split) {
            System.out.println(s);
        }
    }

    @org.junit.Test
    public void h3a() {
        String  haha = (String) MyProperties.getInstance().getPro().get("haha");
        System.out.println(haha);
    }
}

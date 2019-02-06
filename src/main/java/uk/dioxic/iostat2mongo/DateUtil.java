package uk.dioxic.iostat2mongo;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

public class DateUtil {

    private static Pattern datePattern = Pattern.compile("\\d{1,2}/\\d{1,2}/\\d{1,2}\\s\\d{2}:\\d{2}:\\d{2}"); // 12/16/18 15:00:10
    private static DateTimeFormatter df = DateTimeFormatter.ofPattern("MM/dd/yy HH:mm:ss");

    public static boolean isDate(String s) {
        return datePattern.matcher(s).matches();
    }

    public static LocalDateTime parse(String date) {
        return LocalDateTime.parse(date, df);
    }
}

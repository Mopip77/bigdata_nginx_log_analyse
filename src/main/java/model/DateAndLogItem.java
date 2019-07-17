package model;

public class DateAndLogItem {
    private Long date;
    private LogItem logItem;

    public DateAndLogItem(Long dateBase, String ip, Long date, String uri, Integer statusCode, Integer flux) {
        this.date = dateBase;
        this.logItem = new LogItem(ip, date, uri, statusCode, flux);
    }

    public Long getDate() {
        return date;
    }

    public void setDate(Long date) {
        this.date = date;
    }

    public LogItem getLogItem() {
        return logItem;
    }

    public void setLogItem(LogItem logItem) {
        this.logItem = logItem;
    }
}

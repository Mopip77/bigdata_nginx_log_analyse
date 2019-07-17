package model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LogItem implements WritableComparable {
    private String ip;
    private Long date;
    private String uri;
    private Integer statusCode;
    private Integer flux;

    public LogItem(String ip, Long date, String uri, Integer statusCode, Integer flux) {
        this.ip = ip;
        this.date = date;
        this.uri = uri;
        this.statusCode = statusCode;
        this.flux = flux;
    }

    public LogItem() {
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getDate() {
        return date;
    }

    public void setDate(Long date) {
        this.date = date;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
    }

    public Integer getFlux() {
        return flux;
    }

    public void setFlux(Integer flux) {
        this.flux = flux;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(ip);
        out.writeLong(date);
        out.writeUTF(uri);
        out.writeInt(statusCode);
        out.writeInt(flux);
    }

    public void readFields(DataInput in) throws IOException {
        ip = in.readUTF();
        date = in.readLong();
        uri = in.readUTF();
        statusCode = in.readInt();
        flux = in.readInt();
    }

    @Override
    public String toString() {
        return ip + '\t' + date + '\t' + uri + '\t' + statusCode + '\t' + flux;
    }

    public int compareTo(Object o) {
        return 0;
    }
}

package model;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateAndString implements WritableComparable<DateAndString> {
    private Long date;
    private String item;

    public DateAndString(Long date, String item) {
        this.date = date;
        this.item = item;
    }

    public DateAndString(){ }

    public Long getDate() {
        return date;
    }

    public void setDate(Long date) {
        this.date = date;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(date);
        out.writeUTF(item);
    }

    public void readFields(DataInput in) throws IOException {
        date = in.readLong();
        item = in.readUTF();
    }

    @Override
    public String toString() {
        return date + "\t" + item;
    }

    public int compareTo(DateAndString o) {
        if (date.compareTo(o.getDate()) == 0) {
            return item.compareTo(o.getItem());
        }
        return date.compareTo(o.getDate());
    }
}

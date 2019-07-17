package model;

import exception.NoOverrideMethod;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class TimePeriodAndSomething<T extends TimePeriodAndSomething, V extends WritableComparable> implements WritableComparable<T> {

    protected Integer period;
    protected V item;

    public TimePeriodAndSomething(Integer period, V item) {
        this.period = period;
        this.item = item;
    }

    public TimePeriodAndSomething() {
    }

    public void setTimePeriod(Integer timePeriod) {
        this.period = timePeriod;
    }

    public void setPeriod(Integer period) {
        this.period = period;
    }

    public V getItem() {
        return item;
    }

    public void setItem(V item) {
        this.item = item;
    }

    public Integer getPeriod() {
        return period;
    }
}

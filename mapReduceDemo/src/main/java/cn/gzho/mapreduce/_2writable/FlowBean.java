package cn.gzho.mapreduce._2writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author gzho
 * @version 1.0.0
 * @describe hadoop序列化测试bean 统计手机上下行流量
 * 1.定义类实现writable接口
 * 2.重写序列化和反序列化方法
 * 3.重写空参数构造
 * 4.toString方法用于打印输出
 * @updateTime 2021-06-14 11:19 上午
 * @since 2021-06-14 11:19 上午
 */
public class FlowBean implements Writable {

    private Long upFlow;

    private Long downFlow;

    private Long sumFlow;

    public FlowBean() {
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = getUpFlow() + getDownFlow();
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();

    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}

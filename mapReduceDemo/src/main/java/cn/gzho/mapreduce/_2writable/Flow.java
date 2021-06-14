package cn.gzho.mapreduce._2writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author gzho
 * @version 1.0.0
 * @describe
 * @updateTime 2021-06-14 11:30 上午
 * @since 2021-06-14 11:30 上午
 */
public class Flow {

    public static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

        Text text = new Text();

        FlowBean flowBean = new FlowBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //1.获取一行数据
            //eg:1   18968129999   192.196.100.1 www.bilibili.com    299    29999  200
            String line = value.toString();

            //2.切割
            String[] split = line.split(" ");

            //3.抓取想要的数据
            String mobileName = split[1];
            Long upFlow = Long.valueOf(split[split.length - 3]);
            Long downFlow = Long.valueOf(split[split.length - 2]);

            //
            text.set(mobileName);
            flowBean.setUpFlow(upFlow);
            flowBean.setDownFlow(downFlow);
            flowBean.setSumFlow();
            context.write(text, flowBean);
        }
    }

    public static class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

        Text text = new Text();

        FlowBean flowBean = new FlowBean();

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

            Long upFlow = 0L;
            Long downFlow = 0L;
            Long sumFlow = 0L;

            //1.循环遍历集合
            for (FlowBean value : values) {
                upFlow += value.getUpFlow();
                downFlow += value.getDownFlow();
                sumFlow += value.getSumFlow();
            }

            flowBean.setUpFlow(upFlow);
            flowBean.setDownFlow(downFlow);
            flowBean.setSumFlow(sumFlow);

            context.write(key, flowBean);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //1.获取Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.设置jar包路径
        job.setJarByClass(Flow.class);

        //3.关联mapper和reducer
        job.setMapperClass(Flow.FlowMapper.class);
        job.setReducerClass(Flow.FlowReducer.class);

        //4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5.设置reduce输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //6.设置输入路径和输出路径
            FileInputFormat.setInputPaths(job, new Path("/Users/ameng/Documents/hadoop/mobileflow/phone_data.txt"));
            FileOutputFormat.setOutputPath(job, new Path("/Users/ameng/Documents/hadoop/mobileflow/output"));
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

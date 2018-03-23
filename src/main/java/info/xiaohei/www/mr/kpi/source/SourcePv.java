package info.xiaohei.www.mr.kpi.source;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 * <p/>
 * 用户访问来源统计
 */
public class SourcePv {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String[] inPath = new String[]{"hdfs://192.168.0.130:9000/user/root/input/log"};
        String outPath = "hdfs://192.168.0.130:9000/user/root/output/kpi/source";
        Configuration conf = new Configuration();
        String jobName = "source-pv";

        JobInitModel job = new JobInitModel(inPath, outPath, conf, null, jobName
                , SourcePv.class, null, Mapper.class, Text.class, IntWritable.class, null, null, Reducer.class
                , Text.class, IntWritable.class);

        JobInitModel sortJob = new JobInitModel(new String[]{outPath + "/part-*"}, outPath + "/sort", conf, null
                , jobName + "sort", SourcePv.class, null, Mapper.class, Text.class, IntWritable.class, null, null, null, null, null);

        BaseDriver.initJob(new JobInitModel[]{job, sortJob});
    }
}

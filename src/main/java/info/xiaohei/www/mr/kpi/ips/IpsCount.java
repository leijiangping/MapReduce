package info.xiaohei.www.mr.kpi.ips;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 * <p/>
 * 统计每个页面的独立访问ip数
 */
public class IpsCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String[] inPath = new String[]{"hdfs://192.168.0.130:9000/user/root/input/log"};
        String outPath = "hdfs://192.168.0.130:9000/user/root/output/kpi/ips";
        Configuration conf = new Configuration();
        String jobName = "ips";

        JobInitModel job = new JobInitModel(inPath, outPath, conf, null, jobName
                , IpsCount.class, null, Mapper.class, Text.class, Text.class, null, null, Reducer.class
                , Text.class, IntWritable.class);

        JobInitModel sortJob = new JobInitModel(new String[]{outPath + "/part-*"}, outPath + "/sort", conf, null
                , jobName + "sort", IpsCount.class, null, Mapper.class, Text.class, IntWritable.class, null, null, null, null, null);

        BaseDriver.initJob(new JobInitModel[]{job, sortJob});
    }
}

package info.xiaohei.www.mr.kpi.browser;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.JobInitModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by xiaohei on 16/2/21.
 * <p/>
 * 统计用户使用的客户端程序
 */
public class KpiBrowser {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String[] inPath = new String[]{"hdfs://192.168.0.130:9000/user/root/input/log"};
        String outPath = "hdfs://192.168.0.130:9000/user/root/output/kpi/browser";
        Configuration conf = new Configuration();
         //在你的文件地址前自动添加：hdfs://master:9000/  
        conf.set("fs.default.name", "hdfs://192.168.0.130:9000/");  
        conf.set("hadoop.job.user","root");    
        //指定jobtracker的ip和端口号，master在/etc/hosts中可以配置  
        conf.set("mapred.job.tracker","192.168.0.130:9001");  
        String jobName = "browser-pv";

        JobInitModel job = new JobInitModel(inPath, outPath, conf, null, jobName
                , KpiBrowser.class, null, Mapper.class, Text.class, IntWritable.class, null, null, Reducer.class
                , Text.class, IntWritable.class);

        JobInitModel sortJob = new JobInitModel(new String[]{outPath + "/part-*"}, outPath + "/sort", conf, null
                , jobName + "sort", KpiBrowser.class, null, Mapper.class, Text.class, IntWritable.class, null, null, null, null, null);

        BaseDriver.initJob(new JobInitModel[]{job, sortJob});
    }
}

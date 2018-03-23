package info.xiaohei.www.mr.recommend;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import info.xiaohei.www.BaseDriver;
import info.xiaohei.www.HadoopUtil;
import info.xiaohei.www.JobInitModel;
import info.xiaohei.www.mr.recommend.sort.SortData;
import info.xiaohei.www.mr.recommend.sort.SortMapper;
import info.xiaohei.www.mr.recommend.test.RecommendScoreMapper;
import info.xiaohei.www.mr.recommend.test.RecommendScoreReducer;
import info.xiaohei.www.mr.recommend.test.TransferUserScoreMapper;

/**
 * Created by xiaohei on 16/2/24.
 */
public class Recommend {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {
       int flag=1;  //0:use distributed cache,1:use normal map
        Configuration conf = new Configuration();
        //在你的文件地址前自动添加：hdfs://master:9000/  
       conf.set("fs.defaultFS", "hdfs://192.168.0.130:9000/");  
       conf.set("hadoop.job.user","root");    
       //指定jobtracker的ip和端口号，master在/etc/hosts中可以配置  
       conf.set("mapred.job.tracker","192.168.0.130:9001");  
        //计算用户评分矩阵
        String userScoreMatrixInpath = HadoopUtil.HDFS + "/user/root/input/datafile/item.csv";
        String userScoreMatrixOutpath = HadoopUtil.HDFS + "/user/root/output/mr/recommend/userScoreMatrix";
        JobInitModel userScoreMatrixJob = new JobInitModel(new String[]{userScoreMatrixInpath}, userScoreMatrixOutpath
                , conf, null, "CalcUserScoreMatrix", Recommend.class, null, UserScoreMatrixMapper.class, Text.class, Text.class
                , null, null, UserScoreMatrixReducer.class, Text.class, Text.class);

        //计算物品同现矩阵
        String itermOccurrenceOutpath = HadoopUtil.HDFS + "/user/root/output/mr/recommend/itermOccurrenceMatrix";
        JobInitModel itermOccurrenceMatrixJob = new JobInitModel(new String[]{userScoreMatrixOutpath}, itermOccurrenceOutpath
                , conf, null, "CalcItermOccurrenceMatrix", Recommend.class, null, ItermOccurrenceMapper.class, Text.class, IntWritable.class
                , null, null , ItermOccurrenceReducer.class, Text.class, IntWritable.class);

        if (flag==0) {
            //计算推荐结果
            String recommendOutpath = HadoopUtil.HDFS + "/user/root/output/mr/recommend";
            Job job = Job.getInstance(conf);
            job.addCacheFile(new URI(itermOccurrenceOutpath + "/part-r-00000#itermOccurrenceMatri"));
            JobInitModel recommendJob = new JobInitModel(new String[]{userScoreMatrixOutpath}
                    , recommendOutpath, conf, job, "recommend", Recommend.class, null, RecommendMapper.class, Text.class, DoubleWritable.class
                    , null, null, RecommendReducer.class, Text.class, Text.class);

            String sortOutpath = HadoopUtil.HDFS + "/user/root/output/mr/recommend/sortedResult";
            JobInitModel sortJob = new JobInitModel(new String[]{recommendOutpath}
                    , sortOutpath, conf, null, "sortRecommend", Recommend.class, null, SortMapper.class, SortData.class, NullWritable.class
                    , null, null, null, null, null);

            BaseDriver.initJob(new JobInitModel[]{userScoreMatrixJob, itermOccurrenceMatrixJob, recommendJob, sortJob});
        } else {
            String transferUserScoreOutpath = HadoopUtil.HDFS + "/user/root/output/mr/recommend/transferUserScore";
            JobInitModel transferUserScoreJob = new JobInitModel(new String[]{userScoreMatrixOutpath}, transferUserScoreOutpath
                    , conf, null, "TransferUserScore", Recommend.class, null, TransferUserScoreMapper.class, Text.class, Text.class
                    , null, null , null, null, null);

            //计算推荐结果
            String recommendOutpath = HadoopUtil.HDFS + "/user/root/output/mr/recommend";
            JobInitModel recommendJob = new JobInitModel(new String[]{transferUserScoreOutpath, itermOccurrenceOutpath}
                    , recommendOutpath, conf, null, "recommend", Recommend.class, null, RecommendScoreMapper.class, Text.class
                    , Text.class, null, null, RecommendScoreReducer.class, Text.class, Text.class);
           // BaseDriver.initJob(new JobInitModel[]{userScoreMatrixJob, itermOccurrenceMatrixJob});
           // BaseDriver.initJob(new JobInitModel[]{transferUserScoreJob});
           BaseDriver.initJob(new JobInitModel[]{recommendJob});
        }


    }
}

package com.cxlx.avro2hive.mr;

import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author star
 * @create 2019-09-06 11:13
 */
public class AvroLogDriver extends Configuration implements Tool {
    private Configuration conf = null;
    private static final Logger logger = LoggerFactory.getLogger(AvroLogDriver.class);



    public static void main(String[] args) throws Exception {
        for (String arg : args) {
            logger.info("arg:" + arg);
        }
        if (args.length != 3) {
            logger.error("Usage: AvroToHiveDriver <input path> <output path,eg:/user/hive/warehouse/ods.db/> <date eg:20190101>");
            return;
        }
        int run = ToolRunner.run(new AvroLogDriver(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {
//        conf.set("fs.defaultFS","hdfs://hadoop10:9000");
        conf.set("inpath", args[0]);
        conf.set("outpath", args[1]);
        conf.set("logdate", args[2]);

        conf.set("fs.oss.accessKeyId", "LTAIwcPKqog41QMl");
        conf.set("fs.oss.accessKeySecret", "XnRCnAiq49dTIGV385RJR4ivAwsoWD");// 对应的密钥
        conf.set("fs.oss.endpoint", "oss-cn-hangzhou-internal.aliyuncs.com");//访问 OSS 使用的网络


        Job job = Job.getInstance(conf,this.getClass().getSimpleName());

        job.setJarByClass(AvroLogDriver.class);

        job.setMapperClass(AvroLogMapper.class);
        job.setReducerClass(AvroLogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroKeyInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 128);

        initInpath(job);
        initOutpath(job);

        //通过此配置可以不再产生默认的空文件
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    private void initOutpath(Job job) throws IOException {
//        Path outPath = new Path(conf.get("outpath"));
        Path outPath = new Path("/user/hive/warehouse/ods.db/xxx/19000101/");//临时给一个不用的目录，因为最终输出的目录无法确定

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
    }

    private void initInpath(Job job) throws IOException {
        String inpath = conf.get("inpath");

        Path inPath = new Path(inpath);

        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(inPath)) {
//            FileInputFormat.setInputPaths(job, inPath);
//            AvroKeyInputFormat.addInputPaths(job, inPath);
            AvroKeyInputFormat.addInputPath(job,inPath);
        } else {
            throw new RuntimeException(inpath + "not exists!");
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}

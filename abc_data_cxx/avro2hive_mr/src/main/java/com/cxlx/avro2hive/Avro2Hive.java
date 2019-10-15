package com.cxlx.avro2hive;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.oss.OSSClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @author star
 * @create 2019-09-09 15:21
 */
public class Avro2Hive extends Configured implements Tool  {
    private Configuration conf = null;
    private static final Logger logger = LoggerFactory.getLogger(Avro2Hive.class);


    public static void main(String[] args) throws Exception {
        for (String arg : args) {
            logger.info("arg:" + arg);
        }
        if (args.length != 3) {
            logger.error("Usage: AvroToHiveDriver <input path> <output path,eg:/user/hive/warehouse/xxx_ods.db/> <date eg:20190101>");
            return;
        }

        int run = ToolRunner.run(new Avro2Hive(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        conf.set("inpath", args[0]);
        conf.set("outpath", args[1]);
        conf.set("logdate", args[2]);
        conf.set("basiceventinfooutpath", args[1]+"basiceventinfo/dt="+args[2]+"/");

        Job job = Job.getInstance(conf, this.getClass().getSimpleName());

        job.setJarByClass(Avro2Hive.class);

        job.setMapperClass(AvroLogMapper.class);
        job.setReducerClass(AvroLogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(AvroKeyInputFormat.class);
        AvroKeyInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 128);

//        AvroKeyInputFormat.setInputPaths(job,new Path(args[0]));
        initInpath(job);
        initOutpath(job);



        //通过此配置可以不再产生默认的空文件
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    private void initOutpath(Job job) throws IOException {
//        Path outPath = new Path(conf.get("outpath"));

        Path outPath = new Path(conf.get("basiceventinfooutpath"));//临时给一个不用的目录，因为最终输出的目录无法确定
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);
    }

    private void initInpath(Job job) throws IOException {
//        oss://trace-log/front/etl_avro_out/20190312/
//        oss://LTAIwcPKqog41QMl:XnRCnAiq49dTIGV385RJR4ivAwsoWD@trace-log.oss-cn-hangzhou-internal.aliyuncs.com/front/etl_avro_out/20190313/part-m-00000.avro

        String inpath = conf.get("inpath");

        logger.info("enter driver initInpath");
        String[] split = inpath.split("/");

        OSSClient ossClient = null;

        try {
            ossClient = EMapReduceOSSUtil.getOssClient(conf);
            //传入的是文件
            if (split[split.length - 1].contains(".")) {
                AvroKeyInputFormat.addInputPath(job, new Path(inpath));
            //传入的是目录
            }else {
                if (ossClient.doesObjectExist(split[2], split[3] + "/" + split[4] + "/" + split[5] + "/")) {
                    StringBuilder inPath = new StringBuilder();
                    List<String> lists = EMapReduceOSSUtil.listCompleteUri(split[2], split[3] + "/" + split[4] + "/" + split[5] + "/", conf, false);
                    for (String list : lists) {
                        AvroKeyInputFormat.addInputPath(job, new Path(inpath+list.substring(list.lastIndexOf("/")+1)));
                        logger.info("uri:" + inpath+list.substring(list.lastIndexOf("/")+1));
                    }
                } else {
                    throw new RuntimeException(inpath + "not exists!");
                }
            }
        } finally {
            ossClient.shutdown();
        }

        logger.info("exit driver initInpath");
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public static class AvroLogMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, Text> {
        MultipleOutputs<Text, Text> multipleOutputs = null;
        private static volatile Map<String, ArrayList<String>> structTableMap = HiveJdbcUtil.getTableInfo();
        Text eventType = new Text();
        Text json = new Text();
        StringBuilder record = new StringBuilder();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logger.info("enter map setup=============================");

            multipleOutputs = new MultipleOutputs<Text, Text>(context);

        }

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            record.delete(0, record.length());
            GenericRecord datum = key.datum();

            JSONObject jsonGene = JSONObject.parseObject(datum.toString());

            Object eventType = jsonGene.get("eventType");
            if (!structTableMap.containsKey(eventType)) {
                logger.error("datum:"+datum+":eventType:"+eventType);
                return;
            }
            ArrayList<String> cols = structTableMap.get(eventType+"");
            Iterator<String> it = structTableMap.keySet().iterator();
//            logger.info(jsonGene.toString());
            for (String col : cols) {
                JSONObject jsonObject = null;

                JSONObject object = jsonGene;
                while (col.contains(".")) {
                    int i = col.indexOf(".");
                    object = JSONObject.parseObject(object.get(col.substring(0,i))+"");
                    if (object == null) {
                        break;
                    }
                    col = col.substring(i+1);
                }
                Object colSimp = "\\N";
                if (object != null) {
                    colSimp = object.get(col);
                }
//                logger.info(col+"::"+colSimp+"::"+object);
                record.append(col+":"+colSimp).append("\001");
            }
//            cols
//            this.eventType.set(eventType + "");



            json.set(record + "");
            context.write(this.eventType, json);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    public static class AvroLogReducer extends Reducer<Text, Text, Text, NullWritable> {
        MultipleOutputs<Text, NullWritable> multipleOutputs = null;
        FileSystem fs = null;
        Configuration conf = null;
        String outPath = null;
        String logDate = null;
        Text outKey = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logger.info("enter reduce setup ==========================");
            multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
            conf = context.getConfiguration();
            super.setup(context);
            outPath = conf.get("outpath");
            logDate = conf.get("logdate");
            System.out.println("输出路径：" + outPath);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            initOutpath(key.toString());
            for (Text value : values) {
                multipleOutputs.write(value, NullWritable.get(), outPath + key.toString().toLowerCase() + "/dt=" + logDate + "/" + key.toString().toLowerCase());
            }
        }

        private void initOutpath(String key) throws IOException {
            Path path = new Path(outPath + key.toString().toLowerCase() + "/dt=" + logDate + "/");

            fs = FileSystem.get(conf);

            if (fs.exists(path)) {
                fs.delete(path, true);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
            if (fs != null) {
                fs.close();
            }
            super.cleanup(context);
        }
    }

}

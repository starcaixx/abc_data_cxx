package com.cxlx.avro2hive;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.oss.OSSClient;
import com.cxlx.avro2hive.mr.HbcLogStructure;
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
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

/**
 * @author star
 * @create 2019-09-09 15:21
 */
public class Avro2Hive extends Configured implements Tool {
    private Configuration conf = null;
    private static final Logger logger = LoggerFactory.getLogger(Avro2Hive.class);

    public static void main(String[] args) throws Exception {
        for (String arg : args) {
            logger.info("arg:" + arg);
        }
        if (args.length != 3) {
            logger.error("Usage: AvroToHiveDriver <input path> <output path,eg:/user/hive/warehouse/ods.db/> <date eg:20190101>");
            return;
        }

        /*String[] split = args[0].split("/");

        Configuration conf = new Configuration();
        conf.set("fs.oss.accessKeyId", "LTAIwcPKqog41QMl");
        conf.set("fs.oss.accessKeySecret", "XnRCnAiq49dTIGV385RJR4ivAwsoWD");
        conf.set("fs.oss.endpoint", "oss-cn-hangzhou-internal.aliyuncs.com");

        List<String> lists = EMapReduceOSSUtil.listCompleteUri(split[2], split[3] + "/" + split[4] + "/" + split[5] + "/", conf, false);
        int run = 0;
        for (String list : lists) {
            System.out.println(list);
            logger.error("list:"+list);
            args[0] = args[0]+list.substring(list.lastIndexOf("/")+1);
            logger.error("args:"+StringUtils.join("\n:",args));
            run = ToolRunner.run(new Avro2Hive(), args);
            if (run==0) {
                continue;
            }else {
                break;
            }
        }*/
        int run = ToolRunner.run(new Avro2Hive(), args);
        System.exit(run);

        /*int run = ToolRunner.run(new Avro2Hive(), args);
        System.exit(run);*/
    }

    @Override
    public int run(String[] args) throws Exception {
        conf.set("inpath", args[0]);
        conf.set("outpath", args[1]);
        conf.set("logdate", args[2]);

//        conf.set("fs.defaultFS","hdfs://172.16.8.249:9000");
//        conf.set("fs.oss.accessKeyId", "LTAIwcPKqog41QMl");
//        conf.set("fs.oss.accessKeySecret", "XnRCnAiq49dTIGV385RJR4ivAwsoWD");// 对应的密钥
//        conf.set("fs.oss.endpoint", "oss-cn-hangzhou-internal.aliyuncs.com");//访问 OSS 使用的网络


        Job job = Job.getInstance(conf,this.getClass().getSimpleName());

        job.setJarByClass(Avro2Hive.class);

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

//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        AvroKeyInputFormat.addInputPath(job,new Path(args[0]));
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
//        oss://trace-log/front/etl_avro_out/20190312/
//        oss://LTAIwcPKqog41QMl:XnRCnAiq49dTIGV385RJR4ivAwsoWD@trace-log.oss-cn-hangzhou-internal.aliyuncs.com/front/etl_avro_out/20190313/part-m-00000.avro
        String[] split = inpath.split("/");

        Path inPath = new Path(inpath);
        logger.info("enter driver initInpath");
        OSSClient ossClient = null;

        try {
            ossClient = EMapReduceOSSUtil.getOssClient(conf);
            if (ossClient.doesObjectExist(split[2],split[3]+"/"+split[4]+"/"+split[5]+"/")) {
//            if (ossClient.doesObjectExist(split[2],split[3]+"/"+split[4]+"/"+split[5]+"/"+split[6])) {
//            FileInputFormat.setInputPaths(job, inPath);
//            AvroKeyInputFormat.addInputPaths(job, inPath);
                AvroKeyInputFormat.addInputPath(job,inPath);
            } else {
                throw new RuntimeException(inpath + "not exists!");
            }
        }finally {
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
        Text eventType = new Text();
        Text json = new Text();
        StringBuilder record = new StringBuilder();
        private static volatile String[][] basicInfo = new String[4][];

        Map<String, Map<String, String>> tableMap = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logger.info("enter map setup=============================");
            tableMap = HbcLogStructure.getInstance().getTableMap();

//        System.out.println("tableMap:"+tableMap);
            Iterator<String> it = tableMap.keySet().iterator();

            StringBuilder basic = new StringBuilder();
            StringBuilder client = new StringBuilder();
            StringBuilder userinfo = new StringBuilder();
            StringBuilder abtest = new StringBuilder();

//        System.out.println(tableMap.get("clickLineComponent"));

            if (it.hasNext()) {
                String next = it.next();
                TreeSet<String> ts = new TreeSet<>();

                for (String field : tableMap.get(next).keySet()) {
                    String[] wholeFieldName = field.split("\\.");
                    if (wholeFieldName.length <= 3) {
                        if (ts.size() != 3 && wholeFieldName.length == 3) {
                            ts.add(wholeFieldName[1]);
                        }
                        if ("client".equalsIgnoreCase(wholeFieldName[1])) {
                            client.append(field.substring(field.lastIndexOf(".") + 1)).append("|");
                        } else if ("userinfo".equalsIgnoreCase(wholeFieldName[1])) {
                            userinfo.append(field.substring(field.lastIndexOf(".") + 1)).append("|");
                        } else if ("abtest".equalsIgnoreCase(wholeFieldName[1])) {
                            abtest.append(field.substring(field.lastIndexOf(".") + 1)).append("|");
                        } else {
                            basic.append(field.replace(".", "")).append("|");
                        }
                    }
                }

                for (String qualify : ts) {
                    if ("client".equalsIgnoreCase(qualify)) {
                        client.insert(0, qualify + "|");
                    } else if ("userinfo".equalsIgnoreCase(qualify)) {
                        userinfo.insert(0, qualify + "|");
                    } else if ("abtest".equalsIgnoreCase(qualify)) {
                        abtest.insert(0, qualify + "|");
                    }
                }

                basicInfo[0] = basic.toString().split("\\|");
                basicInfo[1] = client.toString().split("\\|");
                basicInfo[2] = userinfo.toString().split("\\|");
                basicInfo[3] = abtest.toString().split("\\|");
            }
        }

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            record.delete(0, record.length());
            GenericRecord datum = key.datum();
//        System.out.println("datum:"+datum);

            eventType.set(datum.get("eventType") + "");


            for (int j = 0; j < basicInfo[0].length; j++) {
                Object o = datum.get(basicInfo[0][j]);
                if (o == null) {
                    o = "\\N";
                }
                record.append(o).append("\001");
            }

            for (int i = 1; i < basicInfo.length; i++) {
                JSONObject jsonObject = JSONObject.parseObject(datum.get(basicInfo[i][0]) + "");
                if (jsonObject != null) {
                    for (int j = 1; j < basicInfo[i].length; j++) {
                        if (jsonObject.containsKey(basicInfo[i][j])) {
                            Object o = jsonObject.get(basicInfo[i][j]);
                            if (o == null) {
                                o = "\\N";
                            }
                            record.append(o);
                        }
                        record.append("\001");

                    }
                } else {
                    for (int j = 1; j < basicInfo[i].length; j++) {
                        record.append("\001");
                    }
                }
            }

            Iterator<String> tableInfo = tableMap.get(eventType + "").keySet().iterator();

            String prefix = null;
            String[] wholeFieldInfo = null;

            JSONObject eventObject = null;

            boolean isEventNull = false;

            while (tableInfo.hasNext()) {
                String field = tableInfo.next();
                String[] fieldInfos = field.split("\\.");
                if (fieldInfos.length > 3) {
                    if (prefix == null || "".equals(prefix)) {
                        prefix = fieldInfos[1];
                        wholeFieldInfo = field.substring(1, field.lastIndexOf(".")).split("\\.");
                    } else if (!prefix.equals(fieldInfos[1])) {
                        prefix = fieldInfos[1];
                        wholeFieldInfo = field.substring(1, field.lastIndexOf(".")).split("\\.");
                    }

                    if (eventObject == null && !isEventNull) {
                        eventObject = JSONObject.parseObject(datum.get(wholeFieldInfo[0]) + "");
                        for (int i = 1; i < wholeFieldInfo.length; i++) {
                            if (eventObject == null) {//事件无数据
                                isEventNull = true;
                                break;
                            }
                            eventObject = eventObject.getJSONObject(wholeFieldInfo[i]);
                        }
                    }

                    if (eventObject != null) {
                        Object o = eventObject.get(fieldInfos[fieldInfos.length - 1]);
                        if (o == null) {
                            o = "\\N";
                        }
                        record.append(o);
                    }

                    record.append("\001");
                }
            }

//        System.out.println(record);
            json.set(record+"");
            context.write(eventType, json);
        }
    }

    public static class AvroLogReducer extends Reducer<Text,Text,Text,NullWritable> {
        MultipleOutputs<Text,NullWritable> multipleOutputs=null;
        FileSystem fs = null;
        Configuration conf = null;
        String outPath = null;
        String logDate = null;
        Text outKey=new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logger.info("enter reduce setup ==========================");
            multipleOutputs=new MultipleOutputs<Text,NullWritable>(context);
            conf = context.getConfiguration();
            super.setup(context);
            outPath = conf.get("outpath");
            logDate = conf.get("logdate");
            System.out.println("输出路径："+outPath);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            initOutpath(key.toString());
            for (Text value : values) {
                multipleOutputs.write(value, NullWritable.get(),outPath+key.toString().toLowerCase()+"/dt="+logDate+"/"+key.toString().toLowerCase());
            }
        }

        private void initOutpath(String key) throws IOException {
            Path path = new Path(outPath+key.toString().toLowerCase()+"/dt="+logDate+"/");

            fs = FileSystem.get(conf);

            if (fs.exists(path)) {
                fs.delete(path,true);
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

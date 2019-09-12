package com.cxlx.avro2hive.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * @author star
 * @create 2019-09-07 9:28
 */
public class AvroLogReducer extends Reducer<Text,Text,Text,NullWritable> {
    MultipleOutputs<Text,NullWritable> multipleOutputs=null;
    FileSystem fs = null;
    Configuration conf = null;
    String outPath = null;
    String logDate = null;
    Text outKey=new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
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

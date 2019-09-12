package com.cxlx.avro2hive.mr;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

/**
 * @author star
 * @create 2019-09-06 11:13
 */
public class AvroLogMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, Text> {
    Text eventType = new Text();
    Text json = new Text();
    StringBuilder record = new StringBuilder();
    private static volatile String[][] basicInfo = new String[4][];

    Map<String, Map<String, String>> tableMap = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
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

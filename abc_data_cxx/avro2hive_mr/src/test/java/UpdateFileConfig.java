import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author star
 * @create 2019-09-06 18:34
 */
public class UpdateFileConfig {
    public static void main(String[] args) {
//        String tmp = "oss://trace-log/front/etl_avro_out/20190312/";
        String tmp = "hdfs://172.16.8.249:9000/user/hive/warehouse/ods.db/";
        String[] split = tmp.split("/");
        for (String s : split) {
            System.out.println(s);
        }
        System.out.println(split[2]);
        System.out.println(split[3]+"/"+split[4]+"/"+split[5]+"/");
        /*JSONObject jsonObject = JSONObject.parseObject("{\"clickLineComponent\":{\"lineId\":\"IC1161950002\",\"lineUrl\":\"https://goods.huangbaoche.com/goods/detail/IC1161950002?t=1552358617131\",\"lineTitle\":\"游览京都经典地标，置身百年前的风韵坂道\"}}");


        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            System.out.println(entry.getKey()+":"+entry.getValue());
        }

        for (Map.Entry<String, Object> entry : jsonObject.getJSONObject("clickLineComponent").entrySet()) {
            System.out.println(entry.getKey()+":"+entry.getValue());
        }*/

    }

    private void updateFile(String fileName) {
        BufferedReader bufferedReader = null;
        String line = "";
        StringBuilder sb = new StringBuilder();
        BufferedWriter bufferedWriter = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(new File(fileName)));

            String tmp = "";
            while ((line = bufferedReader.readLine()) != null) {
                if (line.matches(".*\\s+\\w+;.*")) {
                    tmp = line.replaceAll("(\\s+\\w+);", "$1=null;");
                } else {
                    tmp = line;
                }

                sb.append(tmp).append("\n");


            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(new File(fileName)));
            System.out.println(sb.toString());
            bufferedWriter.write(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    public void test1() {

//        updateFile("D:\\hbc\\gitlab\\hbc_log\\hbc_avro_log\\src\\main\\resources\\component\\address\\Address.avdl");
		File f = new File("D:\\hbc\\gitlab\\hbc_log\\hbc_avro_log\\src\\main\\resources");
//		File[] lists = f.listFiles();

		getFiles(f);
        /*
         * for (int i = 0; i < lists.length; i++) { if(lists[i].isFile()) { space +=
         * lists[i].length(); } }
         */
    }

    private void getFiles(File file) {
//		System.out.println(file.getAbsolutePath());
        long sum = 0;
        File[] listFiles = file.listFiles();

        if (listFiles == null) {
            return;
        }
        for (int i = 0; i < listFiles.length; i++) {
            if (listFiles[i].isFile()) {
                System.out.println(listFiles[i].getAbsolutePath() + "::" + listFiles[i].getName());
                if (listFiles[i].getName().endsWith("avdl")) {
                    System.out.println(listFiles[i].getName());
//                    modifyFileContent(listFiles[i].getAbsolutePath(),"","");
                    updateFile(listFiles[i].getAbsolutePath());
                }
//				listFiles.length;
            } else if (listFiles[i].isDirectory()) {
                getFiles(listFiles[i]);
            }
        }
    }
}

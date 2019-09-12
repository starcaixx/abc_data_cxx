package com.cxlx.avro2hive;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class EMapReduceOSSUtil {
    private static String SCHEMA = "oss://";
    private static String AKSEP = ":";
    private static String BKTSEP = "@";
    private static String EPSEP = ".";
    private static String HTTP_HEADER = "http://";

    private static final int MAX_MAKER = 1000;

    //设置标志位 标志是第一次上传还是 拼接上传的
    boolean flag=true;
    static OSSClient ossClient=null;
    static AppendObjectRequest appendObjectRequest=null;
    static AppendObjectResult appendObjectResult=null;
    static Configuration conf=null;
    /**
     * complete OSS uri
     * convert uri like: oss://bucket/path  to  oss://accessKeyId:accessKeySecret@bucket.endpoint/path
     * ossref do not need this
     *
     * @param oriUri original OSS uri
     */

    public static String buildOSSCompleteUri(String oriUri, String akId, String akSecret, String endpoint) {
        if (akId == null) {
            System.err.println("miss accessKeyId");
            return oriUri;
        }
        if (akSecret == null) {
            System.err.println("miss accessKeySecret");
            return oriUri;
        }
        if (endpoint == null) {
            System.err.println("miss endpoint");
            return oriUri;
        }
        int index = oriUri.indexOf(SCHEMA);
        if (index == -1 || index != 0) {
            return oriUri;
        }
        int bucketIndex = index + SCHEMA.length();
        int pathIndex = oriUri.indexOf("/", bucketIndex);
        String bucket = null;
        if (pathIndex == -1) {
            bucket = oriUri.substring(bucketIndex);
        } else {
            bucket = oriUri.substring(bucketIndex, pathIndex);
        }
        StringBuilder retUri = new StringBuilder();
        retUri.append(SCHEMA)
                .append(akId)
                .append(AKSEP)
                .append(akSecret)
                .append(BKTSEP)
                .append(bucket)
                .append(EPSEP)
                .append(stripHttp(endpoint));
        if (pathIndex > 0) {
            retUri.append(oriUri.substring(pathIndex));
        }
        return retUri.toString();
    }

    /**
     * 创建oss客户端连接
     * @return
     */
    public static OSSClient createOssClient(Configuration configuration){
        configuration.set("fs.oss.accessKeyId", "LTAIwcPKqog41QMl");
        configuration.set("fs.oss.accessKeySecret", "XnRCnAiq49dTIGV385RJR4ivAwsoWD");
        configuration.set("fs.oss.endpoint", "oss-cn-hangzhou-internal.aliyuncs.com");
//        configuration.set("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        String endPoint=configuration.get("fs.oss.endpoint");
        String accessKeyId = configuration.get("fs.oss.accessKeyId");
        String accessKeySecret = configuration.get("fs.oss.accessKeySecret");
        conf=configuration;
        return new OSSClient(endPoint, accessKeyId, accessKeySecret);
    }
    public static OSSClient getOssClient(Configuration configuration){
        if(ossClient!=null){
            return ossClient;
        }else{
            return createOssClient(configuration);
        }
    }

    /**
     * 管理oss客户端连接
     * @param ossClient
     */
    public void closeOssClient(OSSClient ossClient){
        if(ossClient!=null){
            ossClient.shutdown();
        }
    }

    /**
     * 以追加的方式向oss上写入文件
     * @param ossClient  oss客户端连接
     * @param bucketName  桶名字
     * @param keyName   要写入的路径  除了桶名字 全部路径
     * @param inputStream   传入输入流 同时需要夹带第一次要写入的数据
     * @param content   非第一次写入的内容
     */
    public  void appendUploadFile(OSSClient ossClient, String bucketName, String keyName, InputStream inputStream, String content){
        ObjectMetadata metadata=new ObjectMetadata();
        metadata.setContentType("text/plain");
        if(flag){
            appendObjectRequest=new AppendObjectRequest(bucketName, keyName, inputStream,metadata);
            appendObjectRequest.setPosition(0L);
            appendObjectResult=ossClient.appendObject(appendObjectRequest);
            flag=false;
        }else{
            appendObjectRequest.setPosition(appendObjectResult.getNextPosition());
            appendObjectRequest.setInputStream(new ByteArrayInputStream(content.getBytes()));
            appendObjectResult=ossClient.appendObject(appendObjectRequest);
        }

    }

    /**
     * 删除oss上的文件夹下的文件同时删除自己文件夹(会删除自己)
     * @param ossClient oss客户端
     * @param bucketName   oss桶名字
     * @param key   全部路径
     */
    public void deleteObject(OSSClient ossClient,String bucketName,String key){

        String inputUri = new String(new StringBuilder(key));
        StringBuilder sb=new StringBuilder(key);
        if(conf!=null){
            String fileName=null;
            List<String> strings = EMapReduceOSSUtil.listCompleteUriAll("trace-log", inputUri, conf);
            for (String completeUris : strings) {
                System.out.println(completeUris);
                String[] split = completeUris.split("/");
                fileName=split[split.length-1];
                sb.append(fileName);
                //delete必须要指向具体的文件才可以
                ossClient.deleteObject("trace-log",sb.toString());
                sb.delete(sb.length()-fileName.length(), sb.length());
            }
            //最后删除自己文件夹
            ossClient.deleteObject(bucketName, sb.toString());


        }
    }

    public static String buildOSSCompleteUri(String oriUri, Configuration conf) {
        String returnPath = buildOSSCompleteUri(oriUri, conf.get("fs.oss.accessKeyId"), conf.get("fs.oss.accessKeySecret"), conf.get("fs.oss.endpoint"));
        return returnPath;
    }

    private static String stripHttp(String endpoint) {
        if (endpoint.startsWith(HTTP_HEADER)) {
            return endpoint.substring(HTTP_HEADER.length());
        }
        return endpoint;
    }

    public static List<String> listCompleteUri(String bucket, String prefix, Configuration conf, boolean isListAll) {
        OSSClient ossClient = new OSSClient(conf.get("fs.oss.endpoint"), conf.get("fs.oss.accessKeyId"), conf.get("fs.oss.accessKeySecret"));
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        if (StringUtils.isNotEmpty(prefix)) {
            listObjectsRequest.setPrefix(prefix);
        }

        listObjectsRequest.setMaxKeys(MAX_MAKER);

        String maker = null;
        List<String> returnList = new ArrayList<String>();
        listObjectsRequest.setMarker(maker);
        ObjectListing objectListing = ossClient.listObjects(listObjectsRequest);
        List<String> completeUris = parseOssUri(bucket, objectListing.getObjectSummaries(), conf);
        if (CollectionUtils.isNotEmpty(completeUris)) {
            if(!isListAll){
                for (String uri : completeUris) {
                    if (!uri.endsWith("/") && !uri.endsWith("_SUCCESS")) {
                        returnList.add(uri);
                    }
                }
            }else{
                for (String uri : completeUris) {
                        returnList.add(uri);
                }
            }

        }

        return returnList;
    }

    /**
     * 判断文件是否存在  需要如果是判断路径是否存在 在后面加上/
     * @param conf
     * @param bucketName
     * @param objectName
     * @return
     */
    public static boolean ossFileExists(Configuration conf, String bucketName, String objectName){
        OSSClient ossClient = getOssClient(conf);
        boolean found = ossClient.doesObjectExist(bucketName, objectName);
        return found;

    }
    public static List<String> listCompleteUriAll(String bucket, String prefix, Configuration conf) {
        OSSClient ossClient = new OSSClient(conf.get("fs.oss.endpoint"), conf.get("fs.oss.accessKeyId"), conf.get("fs.oss.accessKeySecret"));
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        if (StringUtils.isNotEmpty(prefix)) {
            listObjectsRequest.setPrefix(prefix);
        }

        listObjectsRequest.setMaxKeys(MAX_MAKER);

        String maker = null;
        List<String> returnList = new ArrayList<String>();
        listObjectsRequest.setMarker(maker);
        ObjectListing objectListing = ossClient.listObjects(listObjectsRequest);
        List<String> completeUris = parseOssUri(bucket, objectListing.getObjectSummaries(), conf);
        if (CollectionUtils.isNotEmpty(completeUris)) {
            for (String uri : completeUris) {
                    returnList.add(uri);
            }
        }

        return returnList;
    }
    private static List<String> parseOssUri(String bucketName, List<OSSObjectSummary> objectSummaries, Configuration conf) {
        if (CollectionUtils.isEmpty(objectSummaries)) {
            return new ArrayList<String>();
        }

        String akId = conf.get("fs.oss.accessKeyId");
        String akSecret = conf.get("fs.oss.accessKeySecret");
        String endpoint = conf.get("fs.oss.endpoint");

        String prefix = "oss://" + bucketName + "/";

        List<String> returnList = new ArrayList<String>();
        for (OSSObjectSummary item : objectSummaries) {
            String oriUri = prefix + item.getKey();
            String ossCompleteUri = buildOSSCompleteUri(oriUri, akId, akSecret, endpoint);
            returnList.add(ossCompleteUri);
        }

        return returnList;
    }

    public static void uploadFile(Configuration conf, String bucketName, String objectName, String content) {
        // Endpoint以杭州为例，其它Region请按实际情况填写。

        String endpoint = conf.get("fs.oss.endpoint");
        // 阿里云主账号AccessKey拥有所有API的访问权限，风险很高。强烈建议您创建并使用RAM账号进行API访问或日常运维，请登录 https://ram.console.aliyun.com 创建RAM账号。
        String accessKeyId = conf.get("fs.oss.accessKeyId");
        String accessKeySecret = conf.get("fs.oss.accessKeySecret");

        // 创建OSSClient实例。
        OSSClient ossClient = new OSSClient(endpoint, accessKeyId, accessKeySecret);


        ossClient.putObject(bucketName, objectName, new ByteArrayInputStream(content.getBytes()));

        // 关闭OSSClient。
        ossClient.shutdown();
    }

    public static void renameToAvroSchemaFileFormat(String bucket, String prefix, Configuration conf) {
        OSSClient ossClient = new OSSClient(conf.get("fs.oss.endpoint"), conf.get("fs.oss.accessKeyId"), conf.get("fs.oss.accessKeySecret"));
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucket);
        if (StringUtils.isNotEmpty(prefix)) {
            listObjectsRequest.setPrefix(prefix);
        }

        listObjectsRequest.setMaxKeys(MAX_MAKER);

        String maker = null;
        listObjectsRequest.setMarker(maker);
        ObjectListing objectListing = ossClient.listObjects(listObjectsRequest);
        for (OSSObjectSummary item : objectListing.getObjectSummaries()) {
            String oriUri = item.getKey();
            if (oriUri.endsWith("/")) {
                continue;
            }
            String[] splieArray = oriUri.split("-");
            ossClient.copyObject(bucket, oriUri, bucket, splieArray[0]);
            ossClient.deleteObject(bucket, oriUri);
        }

    }

    public static String readOSSFile(OSSClient  ossClient,String bucketName,String objectName) throws IOException {
        OSSObject object = ossClient.getObject(bucketName, objectName);
        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(object.getObjectContent()));
        StringBuffer stringBuffer=new StringBuffer();
        while(true){
            String line=bufferedReader.readLine();
            if(line==null){
                break;
            }
            if(line.contains("所属eventType为")){
                stringBuffer.append("</br>");
            }
            stringBuffer.append(line);

        }
        return stringBuffer.toString();
    }

    /**
     * 昨天
     * @param bucketName
     * @param objectName
     * @param conf
     * @return
     */
    public static List<String> getYesterdayObjectNameList(String bucketName,String objectName,Configuration conf) {
        Date date = new Date();
        //正常是昨天~  现修改成-2天
        Date yesterday = DateUtils.addDays(date, -1);
//        Date yesterday=new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        String yesterDay = simpleDateFormat.format(yesterday);
        List<String> objectList=new ArrayList<>();
//        List<String> strings = EMapReduceOSSUtil.listCompleteUri("trace-log", "front/etl_log_temp_out/"+yesterDay+"/", conf, false);
        List<String> strings = EMapReduceOSSUtil.listCompleteUri(bucketName, objectName + yesterDay + "/", conf, false);
        for (String lists :
                strings) {
            String[] fileSplit = lists.split("/");
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 3; i < fileSplit.length; i++) {
                stringBuilder.append(fileSplit[i]).append("/");
            }
            stringBuilder.delete(stringBuilder.length() - 1, stringBuilder.length());
            objectList.add(stringBuilder.toString());
        }
        return objectList;
    }
    public static List<String> getYesterdayObjectNameList(String bucketName, String objectName, Configuration conf, int days) {
        //days以当天为准  -1  代表昨天
        Date date = new Date();
        //正常是昨天~  现修改成-2天
        Date yesterday = DateUtils.addDays(date, days);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        String yesterDay = simpleDateFormat.format(yesterday);
        List<String> objectList=new ArrayList<>();
//        List<String> strings = EMapReduceOSSUtil.listCompleteUri("trace-log", "front/etl_log_temp_out/"+yesterDay+"/", conf, false);
        List<String> strings = EMapReduceOSSUtil.listCompleteUri(bucketName, objectName + yesterDay + "/", conf, false);
        for (String lists :
                strings) {
            String[] fileSplit = lists.split("/");
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 3; i < fileSplit.length; i++) {
                stringBuilder.append(fileSplit[i]).append("/");
            }
            stringBuilder.delete(stringBuilder.length() - 1, stringBuilder.length());
            objectList.add(stringBuilder.toString());
        }
        return objectList;
    }
    /*static{
        Configuration conf = new Configuration();
        conf.set("fs.oss.accessKeyId", "LTAIwcPKqog41QMl");
        conf.set("fs.oss.accessKeySecret", "XnRCnAiq49dTIGV385RJR4ivAwsoWD");
        conf.set("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        EMapReduceOSSUtil eMapReduceOSSUtil=new EMapReduceOSSUtil();
        OSSClient ossClient = EMapReduceOSSUtil.createOssClient(conf);
        try {
            String s = EMapReduceOSSUtil.readOSSFile(ossClient, "trace-log", "front/checkavsc/BaseLog.avsc");
            JSONObject jsonObject = JSONObject.parseObject(s);
            System.out.println(jsonObject.toJSONString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.oss.accessKeyId", "LTAIwcPKqog41QMl");
        conf.set("fs.oss.accessKeySecret", "XnRCnAiq49dTIGV385RJR4ivAwsoWD");
        conf.set("fs.oss.endpoint", "oss-cn-hangzhou.aliyuncs.com");
        OSSClient ossClient = EMapReduceOSSUtil.createOssClient(conf);

//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
//        Date date = new Date();
//        Date yestoday = DateUtils.addDays(date, -1);
//        String inputUri = new String(new StringBuilder("front/event_topic/").append(simpleDateFormat.format(yestoday)).append("/"));
//        System.out.println(inputUri);
////        String inputUri2 = "front/event_topic/20180102";
//        List<String> ossCompleteUris = EMapReduceOSSUtil.listCompleteUri("trace-log", inputUri, conf,true);
//        for (String completeUris : ossCompleteUris) {
//            System.out.println(completeUris);
//        }

/*        boolean b = EMapReduceOSSUtil.ossFileExists(conf, "trace-log", "front/etl_log_temp_out/");
        System.err.println("输出路径是否存在？："+b);
        OSSClient ossClient = EMapReduceOSSUtil.getOssClient(conf);
        EMapReduceOSSUtil eMapReduceOSSUtil=new EMapReduceOSSUtil();
        boolean result = EMapReduceOSSUtil.ossFileExists(conf, "trace-log", "front/etl_log_temp/11188a559c2942b3b58326ef75415bda");
        System.out.println("输入路径是否存在？："+result);

        List<String> strings = EMapReduceOSSUtil.listCompleteUri("trace-log", "front/etl_log_temp/", conf, false);
        System.out.println(strings.size()!=0?"有文件":"没文件");*/

/*        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date();
        Date yesterDay = DateUtils.addDays(date, -1);
        String inputUri=new String(new StringBuilder("front/event_topic/").append(simpleDateFormat.format(yesterDay)).append("/"));
        System.out.println(inputUri);*/


//        eMapReduceOSSUtil.deleteObject(ossClient, "trace-log", "front/etl_log_temp/");
//        String outputUri = new String(new StringBuilder("oss://trace-log/front/etl_log/").append(simpleDateFormat.format(yestoday)));
////        System.err.println("outputUri: " + outputUri);
//        String uri = EMapReduceOSSUtil.buildOSSCompleteUri(outputUri, conf);
//        System.out.println(uri);
//        EMapReduceOSSUtil eMapReduceOSSUtil=new EMapReduceOSSUtil();
//        OSSClient ossClient = EMapReduceOSSUtil.createOssClient(conf);
        /*Date date=new Date();
        Date yesterday = DateUtils.addDays(date, -1);
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyyMMdd");
        String yesterDay=simpleDateFormat.format(yesterday);
        List<String> strings = EMapReduceOSSUtil.listCompleteUri("trace-log", "front/etl_log_temp_out/"+yesterDay+"/", conf, false);
        for (String lists:
             strings) {
            String[] fileSplit = lists.split("/");
            StringBuilder stringBuilder=new StringBuilder();
            for (int i=3;i<fileSplit.length;i++){
                stringBuilder.append(fileSplit[i]).append("/");
            }
            stringBuilder.delete(stringBuilder.length()-1,stringBuilder.length());
            System.out.println(stringBuilder.toString());
        }*/
        /*try{
            String s = EMapReduceOSSUtil.readOSSFile(ossClient, "trace-log", "front/etl_log_temp_out/20190306/part-r-00000");
            System.out.println(s);
        }catch (Exception e){
            e.printStackTrace();
        }*/
//        List<String> yesterdayObjectNameList = EMapReduceOSSUtil.getYesterdayObjectNameList("trace-log", "front/etl_log_temp_out/", conf);
        /*List<String> yesterdayObjectNameList = EMapReduceOSSUtil.getYesterdayObjectNameList("trace-log", "hbc_log/", conf);
        for (String lists:
             yesterdayObjectNameList) {
            System.out.println(lists);
        }
        System.out.println("list size:"+yesterdayObjectNameList.size());*/
        /*SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date();
        Date yesterDay = DateUtils.addDays(date, -1);
        String inputSecond=new String(new StringBuilder("hbc_log").append(simpleDateFormat.format(yesterDay)).append("/"));
        System.out.println(inputSecond);
        List<String> hbclogfileList =EMapReduceOSSUtil.listCompleteUri("trace-log", inputSecond, conf, false);*/
        /*try {
            String s = EMapReduceOSSUtil.readOSSFile(ossClient, "trace-log", "front/checkavsc/BaseLog.avsc");
            JSONObject jsonObject = JSONObject.parseObject(s);
            System.out.println(jsonObject.toJSONString());
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        /*List<String> testgaoyuan =EMapReduceOSSUtil.listCompleteUri("trace-log", "front/test_gaoyuan", conf, false);
        for (String te:
             testgaoyuan) {
            System.out.println(te);
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date();
        Date yesterday=DateUtils.addDays(date, -1);
        String inputUri=new String(new StringBuilder("front/event_topic/").append(simpleDateFormat.format(yesterday)).append("/"));
        List<String> testgaoyuan1 =EMapReduceOSSUtil.listCompleteUri("trace-log", inputUri, conf, false);
        for (String te:
                testgaoyuan1) {
            System.out.println(te);
        }*/
        EMapReduceOSSUtil.uploadFile(conf,"trace-log","front/buildflag/"+"20190327"+"/airflow/badCount.txt", "111");
//        EMapReduceOSSUtil.uploadFile(conf,"trace-log","front/etl_log_bad_out/20190326/result.txt", "11");
    }
}

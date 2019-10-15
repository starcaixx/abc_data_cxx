package com.cxlx.avro2hive;

import com.huangbaoche.logservice.avro.BaseLog;
import org.apache.avro.Schema;

import java.util.*;

/**
 * Created by gaikou on 2019/8/27.
 */
public class HbcLogStructure {

    private HbcLogStructure(){}

    private volatile static Map<String,Map<String,String>> tableMap = null;

    private static class SingleHbcLogStructure{
        private static final HbcLogStructure INSTANCE = new HbcLogStructure();
    }

    public static final HbcLogStructure getInstance() {
        return SingleHbcLogStructure.INSTANCE;
    }

    public Map<String,Map<String,String>> getTableMap() {
        if (tableMap == null) {
            synchronized ("hi8.0"){
                tableMap = new HashMap();
                Map<String, String> typeMap = new LinkedHashMap<>();
                Schema schema = BaseLog.getClassSchema();

                parseRecord(schema,"",typeMap);
                generateHiveTableInfo(typeMap);
            }
        }

        return tableMap;
    }

    private void generateHiveTableInfo(Map<String,String> typeMap){

        Schema schema = BaseLog.getClassSchema();
        List<String> eventType = schema.getField("eventType").schema().getEnumSymbols();


        Set<String> keys = typeMap.keySet();

        Map basicInfoMap = new LinkedHashMap();
        for(String key : keys){
            String [] splitArray= key.split("\\.");
            if (splitArray.length <=2){
                basicInfoMap.put(key, typeMap.get(key));
//                basicInfoMap.put(splitArray[splitArray.length-1], typeMap.get(key));
            }
            if(key.toLowerCase().contains(".userinfo.")){
                basicInfoMap.put(key, typeMap.get(key));
//                basicInfoMap.put(splitArray[splitArray.length-1], typeMap.get(key));
            }
            if(key.toLowerCase().contains(".client.")){
                basicInfoMap.put(key, typeMap.get(key));
//                basicInfoMap.put(splitArray[splitArray.length-1], typeMap.get(key));
            }
            if(key.toLowerCase().contains(".abtest.")){
                basicInfoMap.put(key, typeMap.get(key));
//                basicInfoMap.put(splitArray[splitArray.length-1], typeMap.get(key));
            }
        }

        for(String item : eventType){
            String tableName = item;
            Map temTableMap = new LinkedHashMap();
            temTableMap.putAll(basicInfoMap);
            for(String key : keys) {
                if(key.contains(tableName)){
                    temTableMap.put(key, typeMap.get(key));
//                    temTableMap.put(key.substring(key.lastIndexOf(".")+1), typeMap.get(key));
                }
            }
            tableMap.put(tableName, temTableMap);
        }
    }

    private void parseUnion(Schema schema, String parentName,Map<String,String> typeMap) {
        List<Schema> types = schema.getTypes();

        for (Schema item : types) {
            String schemaTypeName = item.getType().getName();
            String schemaName = item.getName();

            if (schemaTypeName.equals("union")) {
                parseUnion(item, parentName,typeMap);
            } else if (schemaTypeName.equals("null")) {
//                System.out.println("outer null");
            } else if (isBasicType(schemaTypeName)) {
                typeMap.put(parentName, schemaName);
//                System.out.println(parentName +" : " + schemaName);
            } else if (schemaTypeName.equals("record")) {
                parseRecord(item, parentName,typeMap);
            }
        }
    }

    private void parseRecord(Schema schema, String parentName,Map<String,String> typeMap){
        List<Schema.Field> fields = schema.getFields();
        for (Schema.Field fieldItem : fields) {
            String fieldName = fieldItem.name();       // 字段的名称, 如:

            Schema fieldSchema = fieldItem.schema();
            String schemaName = fieldSchema.getName();  // schema 的类型名称， 如:

            if (schemaName.equals("record")) {
                parseRecord(fieldSchema, parentName+"."+fieldName,typeMap);
            }
            if (schemaName.equals("union")) {
                parseUnion(fieldSchema, parentName+"."+fieldName,typeMap);
            } else if (schemaName.equals("null")) {
            } else if (isBasicType(schemaName)) {
                typeMap.put(parentName+"."+fieldName, schemaName);
//                System.out.println(parentName+"."+fieldName +": " + schemaName);
            } else if (schemaName.equalsIgnoreCase("EventType")) {
                typeMap.put(parentName+"."+fieldName, "string");
            }
        }
    }

    private static boolean isBasicType(String fieldName) {
        if (fieldName.equals("string")) {
            return true;
        }
        if (fieldName.equals("long")) {
            return true;
        }
        if (fieldName.equals("int")) {
            return true;
        }
        if (fieldName.equals("float")) {
            return true;
        }
        if (fieldName.equals("double")) {
            return true;
        }
        if (fieldName.equals("bigDecimal")) {
            return true;
        }
        if (fieldName.equals("enum")) {
            return true;
        }
        return false;
    }
}

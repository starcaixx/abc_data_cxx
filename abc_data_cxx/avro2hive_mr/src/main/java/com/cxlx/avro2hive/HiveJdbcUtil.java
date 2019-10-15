package com.cxlx.avro2hive;


import java.sql.*;
import java.util.*;

/**
 * @author star
 * @create 2019-09-16 16:50
 */
public class HiveJdbcUtil {

    public static void main(String[] args) {
        Map<String,ArrayList<String>> info = getTableInfo();
        System.out.println(info.get("viewActivityPage"));
    }

    public static Map<String,ArrayList<String>> getTableInfo() {
        Connection con = null;
        HashMap<String, ArrayList<String>> tableSchemas = null;
        try {
            con = getConnection();
            DatabaseMetaData metaData = con.getMetaData();

            ResultSet tables = metaData.getTables(con.getCatalog(), con.getSchema(), null, new String[]{"TABLE"});

            tableSchemas = new HashMap<>();

            Map<String,Map<String,String>> structTableMap = HbcLogStructure.getInstance().getTableMap();

//            structTableMap.keySet()
            String keys = structTableMap.keySet().toString().replace("[","").replace("]","");

            System.out.println("viewArticlePage:"+structTableMap.get("viewArticlePage"));
            while (tables.next()) {
                String table_name = tables.getString("TABLE_NAME").toLowerCase();
                //换成schema中的大小写表名
                int tabIndex = keys.toLowerCase().indexOf(table_name);
                if (tabIndex == -1) {
                    continue;
                }
//                System.out.println(table_name+"::"+tabIndex+"::"+keys);
                String structTableName = keys.substring(tabIndex, tabIndex + table_name.length());

                Map<String, String> structColumns = structTableMap.get(structTableName);
                String cols = structColumns.keySet().toString().replace("[","").replace("]","");

                ResultSet columns = metaData.getColumns(null, "%", table_name, "%");


                ArrayList<String> column_names = new ArrayList<>();
                while (columns.next()) {
                    String column_name = columns.getString("COLUMN_NAME").toLowerCase();
                    if ("dt".equals(column_name)) {
                        break;
                    }

                    int colIndex = cols.toLowerCase().indexOf("."+column_name);
                    String str = "";
                    if (colIndex == -1) {
                        str = column_name;
                    }else{
                        String substring = cols.substring(0, colIndex + column_name.length()+1);
                        str = substring.substring(substring.lastIndexOf(",") + 1).trim().replaceFirst("\\.","");

                        /*if ("env".equals(column_name)) {
                            System.out.println(column_name+"::"+colIndex+"::"+str+"::"+substring);
                        }*/
                    }

                    column_names.add(str);
//                    column_names.put(column_name,str);
                }

                tableSchemas.put(structTableName.trim(),column_names);
            }

            ArrayList<String> viewArticlePage = tableSchemas.get("viewArticlePage");

            /*for (String s : viewArticlePage) {
                System.out.println(s);
            }*/
//            System.out.println(viewArticlePage.toString());

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }finally {
            HiveJdbcUtil.close(con,null);
        }
        return tableSchemas;
    }

    public static Connection getConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive2://master:10000/oss_ods", "hive", "hive123");
        return conn;
    }

    public static void close(Connection conn, Statement state) {
        // TODO Auto-generated method stub
        if(state != null) {
            try {
                state.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        if(conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static void close(Connection conn, PreparedStatement ps, ResultSet resultSet) {
        // TODO Auto-generated method stub
        if(resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        if(ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        if(conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}

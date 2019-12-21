import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class ModelCubeMonthSyInit {
    private static final String baseURL = "http://10.1.24.203:7070/kylin/api";

    public static void main(String[] args) {
        String month = "202002";
        //
        reloadTable(month);
        //
        createModel(month);
        //
        createCube(month);
    }


    //创建Cube
    public static String createCube(String month) {
        String modelName="m_sy_month_"+month;
        String method = "POST";
        String para = "/cubes";
        String cubeName="c_sy_month_"+month;
        String tableName="sys_cus_sy_"+month;


        String cubeDescData ="{\"name\":\""+cubeName+"\",\"is_draft\":false,\"model_name\":\""+modelName+"\",\"description\":\"\",\"null_string\":null,\"dimensions\":[{\"name\":\"ID\",\"table\":\"SYS_CUSTOMER_SALEFACT\",\"column\":\"ID\",\"derived\":null},{\"name\":\"SDATE\",\"table\":\"SYS_CUSTOMER_SALEFACT\",\"column\":\"SDATE\",\"derived\":null},{\"name\":\"GOODSID\",\"table\":\"SYS_CUSTOMER_SALEFACT\",\"column\":\"GOODSID\",\"derived\":null},{\"name\":\"CUSTOMERID\",\"table\":\"SYS_CUSTOMER_SALEFACT\",\"column\":\"CUSTOMERID\",\"derived\":null},{\"name\":\"ACCOUNTID\",\"table\":\"SYS_CUSTOMER_SALEFACT\",\"column\":\"ACCOUNTID\",\"derived\":null},{\"name\":\"IS_GIFT\",\"table\":\"SYS_CUSTOMER_SALEFACT\",\"column\":\"IS_GIFT\",\"derived\":null},{\"name\":\"PARDATE\",\"table\":\"SYS_CUSTOMER_SALEFACT\",\"column\":\"PARDATE\",\"derived\":null},{\"name\":\"CREATETIME\",\"table\":\""+tableName+"\",\"column\":null,\"derived\":[\"CREATETIME\"]},{\"name\":\"LEASTTIME\",\"table\":\""+tableName+"\",\"column\":\"LEASTTIME\",\"derived\":null},{\"name\":\"ADDFLAG\",\"table\":\""+tableName+"\",\"column\":\"ADDFLAG\",\"derived\":null},{\"name\":\"NORFLAG\",\"table\":\""+tableName+"\",\"column\":\"NORFLAG\",\"derived\":null},{\"name\":\"DELFLAG\",\"table\":\""+tableName+"\",\"column\":\"DELFLAG\",\"derived\":null},{\"name\":\"CUSATTR\",\"table\":\""+tableName+"\",\"column\":\"CUSATTR\",\"derived\":null},{\"name\":\"SELFFLAG\",\"table\":\""+tableName+"\",\"column\":\"SELFFLAG\",\"derived\":null},{\"name\":\"ACCOUNTID\",\"table\":\""+tableName+"\",\"column\":\"ACCOUNTID\",\"derived\":null},{\"name\":\"CONSUMLEVEL\",\"table\":\""+tableName+"\",\"column\":\"CONSUMLEVEL\",\"derived\":null},{\"name\":\"CUSTOMNAME\",\"table\":\""+tableName+"\",\"column\":null,\"derived\":[\"CUSTOMNAME\"]},{\"name\":\"CUSTOMTYPE\",\"table\":\""+tableName+"\",\"column\":\"CUSTOMTYPE\",\"derived\":null},{\"name\":\"ADDRESS\",\"table\":\""+tableName+"\",\"column\":null,\"derived\":[\"ADDRESS\"]},{\"name\":\"CONTACTNAME\",\"table\":\""+tableName+"\",\"column\":null,\"derived\":[\"CONTACTNAME\"]},{\"name\":\"PHONENUM\",\"table\":\""+tableName+"\",\"column\":null,\"derived\":[\"PHONENUM\"]},{\"name\":\"PROVINCENAME\",\"table\":\""+tableName+"\",\"column\":\"PROVINCENAME\",\"derived\":null},{\"name\":\"CITYNAME\",\"table\":\""+tableName+"\",\"column\":\"CITYNAME\",\"derived\":null},{\"name\":\"APPROVEDNO\",\"table\":\"PCD_GOODS\",\"column\":null,\"derived\":[\"APPROVEDNO\"]},{\"name\":\"GOODSNAME\",\"table\":\"PCD_GOODS\",\"column\":\"GOODSNAME\",\"derived\":null},{\"name\":\"STANDSPEC\",\"table\":\"PCD_GOODS\",\"column\":null,\"derived\":[\"STANDSPEC\"]},{\"name\":\"UNIT\",\"table\":\"PCD_GOODS\",\"column\":null,\"derived\":[\"UNIT\"]},{\"name\":\"FORMULA\",\"table\":\"PCD_GOODS\",\"column\":null,\"derived\":[\"FORMULA\"]},{\"name\":\"CLASSNAME\",\"table\":\"PCD_GOODS\",\"column\":\"CLASSNAME\",\"derived\":null},{\"name\":\"VENDOR\",\"table\":\"PCD_GOODS\",\"column\":null,\"derived\":[\"VENDOR\"]},{\"name\":\"IS_OWN\",\"table\":\"PCD_GOODS\",\"column\":\"IS_OWN\",\"derived\":null},{\"name\":\"COMPETENAME\",\"table\":\"PCD_GOODS\",\"column\":\"COMPETENAME\",\"derived\":null},{\"name\":\"SHORTNAME\",\"table\":\"PCD_GOODS\",\"column\":\"SHORTNAME\",\"derived\":null},{\"name\":\"SHORTVENDOR\",\"table\":\"PCD_GOODS\",\"column\":\"SHORTVENDOR\",\"derived\":null},{\"name\":\"BUSINESS_NAME\",\"table\":\"SYS_CUSTOMER_BUS\",\"column\":null,\"derived\":[\"BUSINESS_NAME\"]}],\"measures\":[{\"name\":\"_COUNT_\",\"function\":{\"expression\":\"COUNT\",\"parameter\":{\"type\":\"constant\",\"value\":\"1\"},\"returntype\":\"bigint\"}},{\"name\":\"SUM1\",\"function\":{\"expression\":\"SUM\",\"parameter\":{\"type\":\"column\",\"value\":\"SYS_CUSTOMER_SALEFACT.PRODUCTNUMBER\"},\"returntype\":\"bigint\"}},{\"name\":\"SUM2\",\"function\":{\"expression\":\"SUM\",\"parameter\":{\"type\":\"column\",\"value\":\"SYS_CUSTOMER_SALEFACT.AMOUNT\"},\"returntype\":\"decimal(19,4)\"}},{\"name\":\"RAW_PRODUCTNUMBER\",\"function\":{\"expression\":\"RAW\",\"parameter\":{\"type\":\"column\",\"value\":\"SYS_CUSTOMER_SALEFACT.PRODUCTNUMBER\"},\"returntype\":\"raw\"}},{\"name\":\"RAW_AMOUNT\",\"function\":{\"expression\":\"RAW\",\"parameter\":{\"type\":\"column\",\"value\":\"SYS_CUSTOMER_SALEFACT.AMOUNT\"},\"returntype\":\"raw\"}}],\"dictionaries\":[],\"rowkey\":{\"rowkey_columns\":[{\"column\":\"SYS_CUSTOMER_SALEFACT.ID\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\"SYS_CUSTOMER_SALEFACT.SDATE\",\"encoding\":\"date\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\"SYS_CUSTOMER_SALEFACT.GOODSID\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\"SYS_CUSTOMER_SALEFACT.CUSTOMERID\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\"SYS_CUSTOMER_SALEFACT.ACCOUNTID\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\"SYS_CUSTOMER_SALEFACT.PARDATE\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\""+tableName+".LEASTTIME\",\"encoding\":\"date\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\""+tableName+".ADDFLAG\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\""+tableName+".NORFLAG\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\""+tableName+".DELFLAG\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\""+tableName+".CUSATTR\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\""+tableName+".SELFFLAG\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\""+tableName+".ACCOUNTID\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\""+tableName+".CONSUMLEVEL\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\""+tableName+".CUSTOMTYPE\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\""+tableName+".PROVINCENAME\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\""+tableName+".CITYNAME\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\"PCD_GOODS.GOODSNAME\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\"PCD_GOODS.CLASSNAME\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\"PCD_GOODS.IS_OWN\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\"PCD_GOODS.COMPETENAME\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\"PCD_GOODS.SHORTNAME\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\"PCD_GOODS.SHORTVENDOR\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false},{\"column\":\"SYS_CUSTOMER_SALEFACT.IS_GIFT\",\"encoding\":\"dict\",\"encoding_version\":1,\"isShardBy\":false}]},\"hbase_mapping\":{\"column_family\":[{\"name\":\"F1\",\"columns\":[{\"qualifier\":\"M\",\"measure_refs\":[\"_COUNT_\",\"SUM1\",\"SUM2\",\"RAW_PRODUCTNUMBER\",\"RAW_AMOUNT\"]}]}]},\"aggregation_groups\":[{\"includes\":[\"SYS_CUSTOMER_SALEFACT.ID\",\"SYS_CUSTOMER_SALEFACT.SDATE\",\"SYS_CUSTOMER_SALEFACT.GOODSID\",\"SYS_CUSTOMER_SALEFACT.CUSTOMERID\",\"SYS_CUSTOMER_SALEFACT.ACCOUNTID\",\"SYS_CUSTOMER_SALEFACT.PARDATE\",\""+tableName+".LEASTTIME\",\""+tableName+".ADDFLAG\",\""+tableName+".NORFLAG\",\""+tableName+".DELFLAG\",\""+tableName+".CUSATTR\",\""+tableName+".SELFFLAG\",\""+tableName+".ACCOUNTID\",\""+tableName+".CONSUMLEVEL\",\""+tableName+".CUSTOMTYPE\",\""+tableName+".PROVINCENAME\",\""+tableName+".CITYNAME\",\"PCD_GOODS.GOODSNAME\",\"PCD_GOODS.CLASSNAME\",\"PCD_GOODS.IS_OWN\",\"PCD_GOODS.COMPETENAME\",\"PCD_GOODS.SHORTNAME\",\"PCD_GOODS.SHORTVENDOR\"],\"select_rule\":{\"hierarchy_dims\":[],\"mandatory_dims\":[],\"joint_dims\":[[\""+tableName+".SELFFLAG\",\""+tableName+".ACCOUNTID\",\""+tableName+".CONSUMLEVEL\",\""+tableName+".CUSTOMTYPE\",\""+tableName+".PROVINCENAME\",\""+tableName+".CITYNAME\",\"PCD_GOODS.GOODSNAME\",\"PCD_GOODS.CLASSNAME\",\"PCD_GOODS.IS_OWN\",\"PCD_GOODS.COMPETENAME\",\"PCD_GOODS.SHORTNAME\"]]}}],\"signature\":\"WBoZM0Whwhg+CDcN9J3tXw==\",\"notify_list\":[],\"status_need_notify\":[\"ERROR\",\"DISCARDED\",\"SUCCEED\"],\"partition_date_start\":0,\"partition_date_end\":3153600000000,\"auto_merge_time_ranges\":[604800000,2419200000],\"volatile_range\":0,\"retention_range\":0,\"engine_type\":4,\"storage_type\":2,\"override_kylin_properties\":{},\"cuboid_black_list\":[],\"parent_forward\":3,\"mandatory_dimension_set_list\":[],\"snapshot_table_desc_list\":[]}";
        cubeDescData = cubeDescData.replaceAll("\"", "\\\\\"");
        cubeDescData = cubeDescData.replaceAll("[\r\n]", "");
        cubeDescData = cubeDescData.trim();
        String body = "{" + "\"cubeDescData\":" + "\"" + cubeDescData + "\"" +
                ",\"cubeName\" :"+ "\""+cubeName+"\""+
                ",\"project\" :  \"sys_customer\"" +
                "}";
        System.out.println("cubename:" + cubeName);
        return excute(para, method, body);
    }

    //创建model
    public static String createModel(String month) {
        String method = "POST";
        String para = "/models";
        String tableName="sys_customer.sys_cus_sy_"+month;
        String alineName="sys_cus_sy_"+month;
        String modelName="m_sy_month_"+month;
        String joinColum1=alineName+".CUSTOMERID";
        String joinColum2=alineName+".ACCOUNTID";
        String modelDescData ="{\"name\": "+"\""+modelName+"\""+",   \"owner\": \"ADMIN\",   \"is_draft\": false,   \"description\": \"\",   \"fact_table\": \"SYS_CUSTOMER.SYS_CUSTOMER_SALEFACT\",   \"lookups\": [     {       \"table\":"  +"\"" +tableName+"\"" + ",       \"kind\": \"LOOKUP\",       \"alias\": \""+alineName+"\",       \"join\": {         \"type\": \"inner\",         \"primary_key\": [           "+"\""+joinColum2+"\""+",           "+"\""+joinColum1+"\""+"         ],         \"foreign_key\": [           \"SYS_CUSTOMER_SALEFACT.ACCOUNTID\",           \"SYS_CUSTOMER_SALEFACT.CUSTOMERID\"         ]       }     },     {       \"table\": \"SYS_CUSTOMER.PCD_GOODS\",       \"kind\": \"LOOKUP\",       \"alias\": \"PCD_GOODS\",       \"join\": {         \"type\": \"inner\",         \"primary_key\": [           \"PCD_GOODS.GOODSID\"         ],         \"foreign_key\": [           \"SYS_CUSTOMER_SALEFACT.GOODSID\"         ]       }     },     {       \"table\": \"SYS_CUSTOMER.SYS_CUSTOMER_BUS\",       \"kind\": \"LOOKUP\",       \"alias\": \"SYS_CUSTOMER_BUS\",       \"join\": {         \"type\": \"inner\",         \"primary_key\": [           \"SYS_CUSTOMER_BUS.ACCOUNTID\"         ],         \"foreign_key\": [           \"SYS_CUSTOMER_SALEFACT.ACCOUNTID\"         ]       }     }   ],   \"dimensions\": [     {       \"table\": \"SYS_CUSTOMER_SALEFACT\",       \"columns\": [         \"ID\",         \"SDATE\",         \"GOODSID\",         \"CUSTOMERID\",         \"ACCOUNTID\",         \"PARDATE\",         \"IS_GIFT\"       ]     },     {       \"table\": "+"\""+alineName+"\""+",       \"columns\": [         \"CUSTOMERID\",         \"LEASTTIME\",         \"ADDFLAG\",         \"NORFLAG\",         \"DELFLAG\",         \"CUSATTR\",         \"SELFFLAG\",         \"ACCOUNTID\",         \"CREATETIME\",         \"CONSUMLEVEL\",         \"CUSTOMNAME\",         \"CUSTOMTYPE\",         \"ADDRESS\",         \"CONTACTNAME\",         \"PHONENUM\",         \"CITYNAME\",         \"PROVINCENAME\"       ]     },     {       \"table\": \"PCD_GOODS\",       \"columns\": [         \"COMPETENAME\",         \"SHORTNAME\",         \"IS_OWN\",         \"SHORTVENDOR\",         \"VENDOR\",         \"CLASSNAME\",         \"GOODSNAME\",         \"GOODSID\",         \"APPROVEDNO\",         \"STANDSPEC\",         \"UNIT\",         \"FORMULA\"       ]     },     {       \"table\": \"SYS_CUSTOMER_BUS\",       \"columns\": [         \"ACCOUNTID\",         \"BUSINESS_NAME\"       ]     }   ],   \"metrics\": [     \"SYS_CUSTOMER_SALEFACT.AMOUNT\",     \"SYS_CUSTOMER_SALEFACT.PRODUCTNUMBER\"   ],   \"filter_condition\": \"\",   \"partition_desc\": {     \"partition_date_column\": \"SYS_CUSTOMER_SALEFACT.SDATE\",     \"partition_time_column\": null,     \"partition_date_start\": 0,     \"partition_date_format\": \"yyyy-MM-dd\",     \"partition_time_format\": \"HH:mm:ss\",     \"partition_type\": \"APPEND\",     \"partition_condition_builder\": \"org.apache.kylin.metadata.model.PartitionDesc$DefaultPartitionConditionBuilder\"   },   \"capacity\": \"MEDIUM\" }";
        modelDescData = modelDescData.replaceAll("\"", "\\\\\"");
        modelDescData = modelDescData.replaceAll("[\r\n]", " ");

        modelDescData = modelDescData.trim();
        String body = "{" + "\"modelDescData\":" + "\"" + modelDescData + "\"" +
                ",\"modelName\" :" + "\""+modelName+"\""+
                ",\"project\" :  \"sys_customer\"" +
                "}";
        System.out.println("modelname:" + modelName);
        return excute(para, method, body);
    }

    /*
    加载数据
     */
    public static String reloadTable(String month){
        String method="POST";
        String para="/tables/sys_customer.sys_customer_salefact,sys_customer.sys_cus_sy_"+month +",/sys_customer";
        String tableData ="{\"calculate\":true}";
        tableData = tableData.replaceAll("\"", "\\\\\"");
        tableData = tableData.replaceAll("[\r\n]", " ");

        tableData = tableData.trim();
        String body="{" + "\"tableData\":" + "\"" + tableData + "\"" +
                ",\"project\" :  \"sys_customer\"" +
                "}";
        System.out.println("tableaname:sys_cus_sy_"+month);

        return excute(para,method,body);
    }



    private static String excute(String para, String method, String body) {
        StringBuilder out = new StringBuilder();
        try {
            URL url = new URL(baseURL + para);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(method);
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Basic QURNSU46S1lMSU4=");
            connection.setRequestProperty("Content-Type", "application/json");
            if (body != null) {
                byte[] outputInBytes = body.getBytes("UTF-8");
                OutputStream os = connection.getOutputStream();
                os.write(outputInBytes);
                os.close();
            }
            InputStream content = connection.getInputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(content));
            String line;
            while ((line = in.readLine()) != null) {
                out.append(line);
            }
            in.close();
            connection.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return out.toString();
    }

    //获取日期
    public static String getDate(Integer day){
        SimpleDateFormat dateFormat  = new SimpleDateFormat("yyyyMMdd");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -day);
        String date = dateFormat.format(cal.getTime());
        return date;
    }

    /*
  获取昨天所属的月份
   */
    public static String  getYesterdayMonth() {
        SimpleDateFormat dateFormat  = new SimpleDateFormat("yyyyMM");
        Calendar cal  = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        String yesterday = dateFormat.format(cal.getTime());
        return yesterday;
    }

}




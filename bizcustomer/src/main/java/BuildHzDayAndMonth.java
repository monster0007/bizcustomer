import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.commons.codec.binary.Base64;

/**
 * 每天实时跑 在hue   kylin_bulid
 */

public class BuildHzDayAndMonth {
    static String ACCOUNT = "ADMIN";
    static String PWD = "KYLIN";
    static  String baseURL="http://10.1.24.124:7070/kylin/api";

    public static void main(String[] args) {
        try {
            String endTime=getTimeStamp().toString(); //当前时间戳，加8小时
            String date=getDate(1); //获取昨天的日期
            String hztableName="sys_cus_hz_"+date; //汇总表
            String sytableName="sys_cus_sy_"+date; //商业表
            reloadTable(hztableName); //加载汇总的天的数据
            reloadTable(sytableName); //加载商业的天的数据
            String hzCubePath=baseURL+"/cubes/c_hz_day_"+date+"/build"; //天的汇总的Cube路径
            String syCubePath=baseURL+"/cubes/c_sy_day_"+date+"/build";//天的商业的Cube路径
            Put(hzCubePath,"{\"startTime\": 1475280000000,\"endTime\":"+ endTime +",\"buildType\": \"BUILD\"}");//构建汇总Cube
            Put(syCubePath,"{\"startTime\": 1475280000000,\"endTime\":"+ endTime +",\"buildType\": \"BUILD\"}");//构建商业Cube
            Thread.sleep(600000);
            String yesMonth=getYesterdayMonth();//昨天所属的月份
            String hztableMonth="sys_cus_hz_"+yesMonth;//月份汇总表
            String sytableMonth="sys_cus_sy_"+yesMonth;//月份商业表
            reloadTable(hztableMonth);
            reloadTable(sytableMonth);
            String hzCubeMonthPath=baseURL+"/cubes/c_hz_month_"+yesMonth+"/build";
            String syCubeMonthPath=baseURL+"/cubes/c_sy_month_"+yesMonth+"/build";
            Put(hzCubeMonthPath,"{\"startTime\": 1475280000000,\"endTime\":"+ endTime +",\"buildType\": \"BUILD\"}");
            Put(syCubeMonthPath,"{\"startTime\": 1475280000000,\"endTime\":"+ endTime +",\"buildType\": \"BUILD\"}");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /*
    bulid Cube
     */
    public static String Put(String addr, String params) {
        String result = "";
        try {
            URL url = new URL(addr);
            HttpURLConnection connection = (HttpURLConnection) url
                    .openConnection();
            connection.setRequestMethod("PUT");
            connection.setDoOutput(true);
            String auth = ACCOUNT + ":" + PWD;
            String code = new String(new Base64().encode(auth.getBytes()));
            connection.setRequestProperty("Authorization", "Basic " + code);
            connection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            PrintWriter out = new PrintWriter(connection.getOutputStream());
            out.write(params);
            out.close();
            BufferedReader in;
            try {
                in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            } catch (FileNotFoundException exception) {
                java.io.InputStream err = ((HttpURLConnection) connection)
                        .getErrorStream();
                if (err == null)
                    throw exception;
                in = new BufferedReader(new InputStreamReader(err));
            }
            StringBuffer response = new StringBuffer();
            String line;
            while ((line = in.readLine()) != null)
                response.append(line + "\n");
            in.close();

            result = response.toString();
        } catch (MalformedURLException e) {
            System.err.println(e.toString());
        } catch (IOException e) {
            System.err.println(e.toString());
        }
        return result;
    }

    /*
   加载数据
    */
    public static String reloadTable(String tableName){
        String method="POST";
        String para="/tables/sys_customer.sys_customer_salefact,sys_customer."+tableName +",/sys_customer";
        String tableData ="{\"calculate\":true}";
        tableData = tableData.replaceAll("\"", "\\\\\"");
        tableData = tableData.replaceAll("[\r\n]", " ");

        tableData = tableData.trim();
        String body="{" + "\"tableData\":" + "\"" + tableData + "\"" +
                ",\"project\" :  \"sys_customer\"" +
                "}";
        return excute(para,method,body);
    }


    /*
    加载数据执行
     */
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
            InputStream content = (InputStream) connection.getInputStream();
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

    /*
    获取时间戳，加8小时
     */
    public static Long getTimeStamp() {
      Long timeStamp=System.currentTimeMillis()+8*60*60*1000;
     return timeStamp;
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

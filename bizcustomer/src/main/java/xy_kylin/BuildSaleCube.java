package xy_kylin;

import org.apache.commons.codec.binary.Base64;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;


public class BuildSaleCube {
    static String ACCOUNT = "ADMIN";
    static String PWD = "KYLIN";
    static  String baseURL="http://10.1.24.219:18080/kylin/api";

    public static void main(String[] args) {
        try {
            String endTime=getTimeStamp().toString(); //当前时间戳，加8小时
             reloadTable();//加载数据
           Thread.sleep(6000);
            String suUrl=baseURL+"/cubes/c_su_settle/build";
           String saleUrl=baseURL+"/cubes/c_sale_settle/build";
           Put(suUrl,"{\"startTime\": 1538236800000,\"endTime\":"+ endTime +",\"buildType\": \"BUILD\"}");
           Put(saleUrl,"{\"startTime\":1538236800000,\"endTime\":"+ endTime +",\"buildType\": \"BUILD\"}");
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
                InputStream err = ((HttpURLConnection) connection)
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
    public static String reloadTable(){
        String method="POST";
        String para="/tables/XYDATA.D_SALER,XYDATA.D_GOODS,XYDATA.D_CUSTOMER,XYDATA.D_SALE_AREA_TARGET,XYDATA.D_SUPPLYER,XYDATA.D_BUYER,/xy_analyse";
        String tableData ="{\"calculate\":true}";
        tableData = tableData.replaceAll("\"", "\\\\\"");
        tableData = tableData.replaceAll("[\r\n]", " ");

        tableData = tableData.trim();
        String body="{" + "\"tableData\":" + "\"" + tableData + "\"" +
                ",\"project\" :  \"xy_analyse\"" +
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

}

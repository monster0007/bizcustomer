

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DeleteModelCubeDayHz {


    public static void main(String[] args) {

    }

    private static final String baseURL = "http://10.1.24.124:7070/kylin/api";

    //删除Cube
    public static String createCube(String day) {
        String modelName="test_tmp";
        String method = "POST";
        String para = "/cubes";
        String cubeName="test_tmp";
       // String tableName="sys_cus_hz_"+day;


        String cubeDescData ="disable";
//        cubeDescData = cubeDescData.replaceAll("\"", "\\\\\"");
//        cubeDescData = cubeDescData.replaceAll("[\r\n]", "");
//        cubeDescData = cubeDescData.trim();
        String body = "{" + "\"cubeDescData\":" + "\"" + cubeDescData + "\"" +
                ",\"cubeName\" :"+ "\""+cubeName+"\""+
                ",\"project\" :  \"sys_customer\"" +
                "}";
        return excute(para, method, body);
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
}

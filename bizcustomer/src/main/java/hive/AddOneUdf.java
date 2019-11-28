package hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AddOneUdf extends UDF {
    public static void main(String[] args) {
        long cfr_time=18099*60*24*60*1000;
        System.out.println(cfr_time);
        System.out.println(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(sdf.format(new Date(Long.parseLong(String.valueOf(cfr_time)))));
    }
}

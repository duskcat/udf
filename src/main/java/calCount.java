import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.HashMap;
import java.util.Map;


public class calCount extends UDF
{

    public String evaluate(String scancount,String activepoint)
    {
        Map<String,Integer> fp=StringToMap(activepoint);
        int times=0;
        int activeKeycount=0;
        for (String key:fp.keySet())
        {
            if(fp.get(key)>0)
            {
                times+=fp.get(key);
                activeKeycount+=1;
            }
        }
        String totalscan=String.valueOf(Integer.valueOf(scancount)*activeKeycount);
        String overtimes=String.valueOf(times);
        return totalscan+':'+overtimes;
    }

    public static Map<String,Integer> StringToMap(String jsonStr)
    {
        Map<String,Integer> tras=new HashMap<String, Integer>();
        if(jsonStr!="")
        {
            String[] kv=jsonStr.substring(1,jsonStr.length()-1).split(", ");
            if(kv.length>1)
                for (String str:kv)
                {
                    String[] kvsplite=str.split(": ");
                    tras.put(kvsplite[0],(int)(float)Float.valueOf(kvsplite[1]));
                }
        }
        return tras;
    }
//    public static void main(String[] args)
//    {
//        String jsonStr="{}";
//        String[] kv=jsonStr.substring(1,jsonStr.length()-1).split(", ");
//        if(kv.length>1)
//            System.out.println(kv);
//        Map<String,Integer> tes=new HashMap<String, Integer>();
//        tes.put("aaa",1);
//        System.out.println(tes.toString());
//    }
}

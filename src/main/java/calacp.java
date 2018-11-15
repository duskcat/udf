import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;

public class calacp extends UDF
{
        public String evaluate(String activepoint)
        {
            Map<String,Integer> fp=StringToMap(activepoint);
            int activeKeycount=0;
            for (String key:fp.keySet())
            {
                if(fp.get(key)>0)
                {
                    activeKeycount+=1;
                }
            }
            String acp=String.valueOf(activeKeycount);
            return String.valueOf(fp.size())+','+acp;
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
}

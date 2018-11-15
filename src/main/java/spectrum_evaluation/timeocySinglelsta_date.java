package spectrum_evaluation;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.velocity.context.InternalContextAdapter;

import java.lang.reflect.Array;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;


@Description(name = "mostOccrItem", value = "_FUNC_(x) - Returns a object that occures most. "
        + "CAUTION will easily cause Out Of Memmory Exception on large data sets")
/**  *   * @author houzhizhen  * create temporary function mostOccrItem as com.letv.bigdata.hive.udaf.MostOccuItem  *   *    public static enum Mode {    * PARTIAL1: from original data to partial aggregation data: iterate() and  * terminatePartial() will be called.    PARTIAL1,    * PARTIAL2: from partial aggregation data to partial aggregation data:  * merge() and terminatePartial() will be called.    PARTIAL2,    * FINAL: from partial aggregation to full aggregation: merge() and  * terminate() will be called.    FINAL,    * COMPLETE: from original data directly to full aggregation: iterate() and  * terminate() will be called.    COMPLETE  };   */
public class timeocySinglelsta_date extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(mergepoint.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName()
                            + " was passed as parameter 1.");
        }
        return new GenericUDAFMkListEvaluator();
    }

    public static class GenericUDAFMkListEvaluator extends GenericUDAFEvaluator {
        // private PrimitiveObjectInspector inputOI;

        private StandardMapObjectInspector mapOI ;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1 ) {
                //inputOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory
                        .getStandardMapObjectInspector((PrimitiveObjectInspector) ObjectInspectorUtils
                                .getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector),PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            }else if (m == Mode.PARTIAL2) {
                mapOI = (StandardMapObjectInspector) parameters[0];
                return ObjectInspectorFactory
                        .getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            }else if(m == Mode.FINAL){
                mapOI = (StandardMapObjectInspector) parameters[0];
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            }else if(m == Mode.COMPLETE){
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            }else {
                throw new RuntimeException("no such mode Exception");
            }
        }

        static class MkArrayAggregationBuffer implements AggregationBuffer
        {
            Map<String,Integer> container;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException
        {
            ((MkArrayAggregationBuffer) agg).container = new HashMap<String,Integer>();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException
        {
            MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        // Mapside
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException
        {
            Object p = parameters[1];
            float scancount=Float.valueOf(parameters[0].toString());
            if (p != null) {
                MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
                Map<String,Integer> activeFreq=new HashMap<String, Integer>();
                Map<String,Integer> acpoint=StringToMap(p.toString());
                for (String key:acpoint.keySet())
                {
                    float value=(float)acpoint.get(key);
                    if(value/scancount>=0.03)
                        activeFreq.put(key,1);
                }

                myagg.container=mergeMap(myagg.container,activeFreq);
            }
        }

        // Mapside
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException
        {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            Map<String,Integer> ret = new HashMap<String,Integer>(myagg.container);
            return ret;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException
        {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            Map partialResult = mapOI.getMap(partial);
            for (Object key : partialResult.keySet()) {
                if (myagg.container.containsKey(key.toString()) && myagg.container.get(key) != null) {
                    myagg.container.put(key.toString(), Integer.parseInt(partialResult.get(key).toString()) + myagg.container.get(key));
                } else {
                    myagg.container.put(key.toString(), Integer.parseInt(partialResult.get(key).toString()));
                }
            }
        }

        @Override
        public String terminate(AggregationBuffer agg) throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            return myagg.container.toString();
        }


        public static Map<String,Integer> mergeMap(Map<String,Integer> target, Map<String,Integer>plus) {
            Object[] os = plus.keySet().toArray();
            String key;
            for (int i = 0; i < os.length; i++) {
                key = (String) os[i];
                if (target.containsKey(key))
                {
                    target.put(key, target.get(key) + plus.get(key));
                }
                else target.put(key, plus.get(key));
            }
            return target;
        }

        public static Map<String,Integer> StringToMap(String jsonStr)
        {
            Map<String,Integer> tras=new HashMap<String, Integer>();
            if(jsonStr!=""&&jsonStr.length()>2)
            {
                String[] kv=jsonStr.substring(1,jsonStr.length()-1).split(",");
                if(kv.length>1)
                    for (String str:kv)
                    {
                        String[] kvsplite=str.split(":");
                        tras.put(kvsplite[0],(int)(float)Float.valueOf(kvsplite[1]));
                    }
            }
            return tras;
        }
    }
}
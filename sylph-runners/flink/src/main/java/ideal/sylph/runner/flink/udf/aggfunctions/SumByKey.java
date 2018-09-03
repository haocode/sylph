package ideal.sylph.runner.flink.udf.aggfunctions;


import com.google.common.collect.Maps;
import ideal.sylph.runner.flink.udf.UdfAnnotation;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.Map;

/**
 * Accumulator for latest.
 */

/**
 * 获取最近的数据.
 */
@UdfAnnotation(name = "sum_by_key")
public class SumByKey extends AggregateFunction<Integer, Map<Object, Integer>> {

    @Override
    public Map<Object, Integer> createAccumulator() {
        return Maps.newHashMap();
    }

    @Override
    public Integer getValue(Map<Object, Integer> src) {
        int sum = 0;
        for (Integer v : src.values()) {
            sum += v;
        }
        return sum;
    }

    public void accumulate(Map<Object, Integer> src, Object key, int value) {
        src.put(key, value);
    }
}
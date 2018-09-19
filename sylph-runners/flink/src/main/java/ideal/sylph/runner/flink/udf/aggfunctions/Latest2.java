package ideal.sylph.runner.flink.udf.aggfunctions;


import ideal.sylph.runner.flink.udf.UdfAnnotation;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.ArrayList;

/**
 * Accumulator for latest.
 */

/**
 * 获取最近的数据.
 */
@UdfAnnotation(name = "latest2")
public class Latest2 extends AggregateFunction<String[], ArrayList<String>> {

    @Override
    public ArrayList<String> createAccumulator() {
        return new ArrayList<String>();
    }

    @Override
    public String[] getValue(ArrayList<String> data) {
        if (data == null || data.size() == 0)
            return null;
        String[] result = new String[data.size()];
        data.toArray(result);
        return result;
    }

    public void accumulate(ArrayList<String> src, Object... input) {
        if (src.size() == 0) {
            addAll(src, input);
        } else {
            String curr_order_by_value = String.valueOf(input[0]);
            String src_order_by_value = String.valueOf(src.get(0));
            if (src_order_by_value.compareTo(curr_order_by_value) < 0) {
                src.clear();
                addAll(src, input);
            }
        }
    }

    public void addAll(ArrayList<String> src, Object[] input) {
        for (Object o : input) {
            // 如果是null，则String.ValueOf会转成字符串null
            src.add(String.valueOf(o));
        }
    }
}
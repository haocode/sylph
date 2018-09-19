package ideal.sylph.runner.flink.udf.aggfunctions;


import ideal.sylph.runner.flink.udf.UdfAnnotation;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Accumulator for latest.
 */

/**
 * 获取最近的数据.
 */
@UdfAnnotation(name = "latest")
public class Latest extends AggregateFunction<Object[], List> {

    @Override
    public List createAccumulator() {
        return new ArrayList();
    }

    @Override
    public Object[] getValue(List data) {
        return data.toArray();
    }

    public void accumulate(List src, Object... input) {
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

    public void addAll(List src, Object[] input) {
        for (Object o : input) {
            src.add(o);
        }
    }
}
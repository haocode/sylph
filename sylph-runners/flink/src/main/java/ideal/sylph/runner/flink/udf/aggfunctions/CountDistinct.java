package ideal.sylph.runner.flink.udf.aggfunctions;


import ideal.sylph.runner.flink.udf.UdfAnnotation;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.HashSet;
import java.util.Set;

/**
 * Accumulator for count_distinct.
 */

/**
 * Weighted Average user-defined aggregate function.
 */
@UdfAnnotation(name = "count_distinct")
public class CountDistinct extends AggregateFunction<Integer, Set> {

    @Override
    public Set createAccumulator() {
        return new HashSet();
    }

    @Override
    public Integer getValue(Set acc) {
        return acc.size();
    }

    public void accumulate(Set set, Object o) {
        set.add(String.valueOf(o));
    }
}
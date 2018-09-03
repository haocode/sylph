package ideal.sylph.runner.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

@UdfAnnotation(name = "unix_timestamp")
public class UnixTimestamp extends ScalarFunction {

    public long eval(Timestamp timestamp) {
        return timestamp.getTime();
    }
}

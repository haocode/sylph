package ideal.sylph.runner.flink.udf;

import org.apache.flink.table.functions.UserDefinedFunction;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author fannk
 * @date 2018/5/18 13:02
 * @comment: todo
 */
public class UdfFactory implements Serializable {
    protected static Logger logger = LoggerFactory.getLogger(UdfFactory.class);
    private static Map<String, UserDefinedFunction> userDefinedFunctionHashMap = null;

    private static void load() {
        userDefinedFunctionHashMap = new HashMap<>();
        Set<Class<?>> classSet = new Reflections("ideal.sylph.runner.flink.udf").getTypesAnnotatedWith(UdfAnnotation.class);
        for (Class c : classSet) {
            if (!c.isAssignableFrom(UserDefinedFunction.class)) {
                String columnName = ((UdfAnnotation) c.getAnnotation(UdfAnnotation.class)).name();
                UserDefinedFunction userDefinedFunction = null;
                try {
                    userDefinedFunction = (UserDefinedFunction) c.newInstance();
                } catch (Exception e) {
                    logger.error("加载UDF失败", e);
                }
                userDefinedFunctionHashMap.put(columnName, userDefinedFunction);
            } else {
                System.out.println("自定义udf需要继承UserDefinedFunction");
            }
        }
    }

    public static Map<String, UserDefinedFunction> getUserDefinedFunctionHashMap() {
        if (userDefinedFunctionHashMap == null) {
            load();
        }
        return userDefinedFunctionHashMap;
    }
}

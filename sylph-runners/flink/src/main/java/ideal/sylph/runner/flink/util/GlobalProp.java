package ideal.sylph.runner.flink.util;

import java.util.Properties;

public class GlobalProp {
    private static Properties properties;

    public static Properties getProperties() {
        return properties;
    }

    public static void setProperties(Properties prop) {
        properties = prop;
    }
    public static Properties getPropertiesWithPrefix(Properties propertiesOld, String prefix) {
        Properties properties1 = new Properties();
        for (String name : propertiesOld.stringPropertyNames()) {
            if (name.startsWith(prefix)) {
                properties1.put(name.replaceFirst(prefix + ".", ""), propertiesOld.get(name));
            }
        }
        return properties1;
    }
}

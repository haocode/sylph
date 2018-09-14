package ideal.sylph.runner.flink.util;

import org.yaml.snakeyaml.Yaml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class YamlUtil {
    public static <T> T loadAs(InputStream input, Class<T> type, Properties confProperties) throws IOException {
        int pos1 = -1;
        StringBuilder sb = new StringBuilder();
        byte[] b = new byte[4096];
        for (int n; (n = input.read(b)) != -1; ) {
            sb.append(new String(b, 0, n));

            // 检查替换
            int newPos1 = 0;
            while (newPos1 >= 0) {
                newPos1 = sb.indexOf("${", pos1 + 1);
                if (newPos1 > 0) {
                    int newPos2 = sb.indexOf("}", newPos1 + 1);
                    if (newPos2 > 0) {
                        String propKeyName = sb.substring(newPos1 + 2, newPos2);
                        if (!confProperties.containsKey(propKeyName)) {
                            throw new RuntimeException("配置文件中找不到指定的key:" + propKeyName);
                        }

                        String value = confProperties.getProperty(propKeyName);
                        sb.replace(newPos1, newPos2 + 1, value == null ? "" : value);

                        // 更新pos1
                        pos1 = newPos1;
                    }
                }
            }
        }

        InputStream newInput = new ByteArrayInputStream(sb.toString().getBytes());
        T t = new Yaml().loadAs(newInput, type);
        newInput.close();
        return t;
    }
}

/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.sylph.runner.flink.actuator;

import ideal.sylph.runner.flink.util.*;
import org.apache.commons.io.FileUtils;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.Test;
import java.io.*;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class FlinkStreamSqlActuatorTest {
    @Test
    public void testSqlSplit()
            throws URISyntaxException, IOException {
        String sqlFilePath = this.getClass().getResource("/stream_test1.sql").getPath();
        String sqlText = FileUtils.readFileToString(new File(sqlFilePath), UTF_8);

        String[] sqlSplit = sqlText.split(";(?=([^\']*\'[^\']*\')*[^\']*$)");
        if (sqlText.toLowerCase().contains("use table ")) {
//            Set<String> tableSet=Stream.of(sqlSplit).filter(sql -> sql.toLowerCase().contains("use table ")).flatMap(sqlfile -> Arrays.stream(sqlfile.split("use table ")
//                    [1].split(","))).collect(Collectors.toSet());

              StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment();
               StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnv);

            Set<String> tableSet= Stream.of(sqlSplit).filter(sql -> sql.toLowerCase().contains("use table ")).map(sqlfile -> sqlfile.split("use table ")[1]).collect(Collectors.toSet());

          Map <String,String> table_type =new HashMap<String,String>();
            for (String table:tableSet) {
               // String[] kv =table.split("\\.");
                JSONObject tablArray   =   JSONObject.parseObject(table);

                tablArray.keySet().stream().forEach(System.out::println);


                for (String map : tablArray.keySet()) {
                    Map value = (Map) tablArray.get(map);

                    try {
                        TableSchemas tableSchema = mapToObject(value, TableSchemas.class);


                        System.out.println(tableSchema.getGroupId() + tableSchema.getOffsetStrategy());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    }
                }



            Properties properties=new Properties();
            try {
                properties.load(KafkaSourceUtil.class.getClassLoader().getResourceAsStream("conf-dev.properties"));

                GlobalProp.setProperties(properties);



                getSchema(properties,table_type);



               // System.out.println(tableSchema.getBootstrapServers());
            } catch (IOException e) {
                e.printStackTrace();
            }




            //table_type.keySet().stream().forEach(System.out::println);

                    //.map(sql -> sql.split(
                  //  "use table")[0].split(",")).toArray(String[]::new);
         //   String[] typeArray = Stream.of(tableArray).map(table -> table.split(".")[0]).toArray(String[]::new);

//        Assert.assertEquals(4, split.length);


        }
    }


    public static <T> T mapToObject(Map<String, Object> map, Class<T> beanClass) throws Exception {
        if (map == null) {
            return null;
        }

        T obj = beanClass.newInstance();
        org.apache.commons.beanutils.BeanUtils.populate(obj, map);

        return obj;
    }

    public static void getSchema(Properties properties, Map<String,String> table_type){

        InputStream in= null;

        for (Map.Entry<String,String> entry :table_type.entrySet()) {
            if (entry.getValue().equals("kafka")) {
                try {


                    in = new FileInputStream("E:\\geely\\code\\sylph\\sylph-runners\\flink\\src\\test\\resources\\kafkaSource\\"+requireNonNull(entry.getKey().split("__")[0],"kakfa 表名称使用不规范")+"\\"+entry.getKey()+".yaml");

                    KafkaSourceSchema tableSchema=YamlUtil.loadAs(in,KafkaSourceSchema.class,properties);

                    System.out.println(tableSchema.getTopic());
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }


        }

    }
}
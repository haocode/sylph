package ideal.sylph.runner.flink.util;

import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static java.util.Objects.requireNonNull;

public class KafkaSourceUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSourceUtil.class);

    public static KafkaSourceSchema loadSchema(String fileName, Properties confProperties) throws IOException {
        String kafkaSourceDirectory = confProperties.getProperty("kafka.source.directory");
        InputStream in;
        String filePath;
        LOGGER.debug("kafkaSourceDirectory" + kafkaSourceDirectory);
        filePath= kafkaSourceDirectory + "/" + fileName;
        in= new FileInputStream(filePath);
        if (in == null) {
            filePath=kafkaSourceDirectory+"/" +requireNonNull(fileName.split("__")[0],"kakfa 表名称使用不规范")+"/"+ fileName;
            in=new FileInputStream(filePath);

        }
        KafkaSourceSchema kafkaSourceSchema = YamlUtil.loadAs(in, KafkaSourceSchema.class, confProperties);
        in.close();
        return kafkaSourceSchema;
    }

    /**
     * 注册flink kafka逻辑表
     *
     * @param streamTableEnvironment
     * @param schemaFileName         kafka topic元数据文件
     * @param confProperties         系统配置文件属性
     * @throws IOException
     * @Param tableSchema            flink表元数据信息
     */
    public static void registerTableSource(StreamTableEnvironment streamTableEnvironment,
                                           String schemaFileName,
                                           ideal.sylph.runner.flink.util.TableSchemas tableSchema,
                                           Properties confProperties) throws IOException {
        KafkaSourceSchema kafkaSourceSchema = loadSchema(schemaFileName+".yaml", confProperties);
        OffsetStrategy offsetStrategy = OffsetStrategy.fromName(tableSchema.getOffsetStrategy());
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", kafkaSourceSchema.getBootstrapServers());
        kafkaProps.setProperty("group.id", tableSchema.getGroupId());

        Map<String, String> aliasMap = new HashMap<>();
        TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
        TableSchemaBuilder jsonSchemaBuilder = TableSchema.builder();
        for (FieldSchema fieldSchema : kafkaSourceSchema.getFieldSchemas()) {
            FieldType fieldType = FieldType.fromTypeName(fieldSchema.getType().toLowerCase());
            if (fieldType == null) {
                throw new RuntimeException("无法识别的字段类型:" + fieldSchema.getType());
            }

            jsonSchemaBuilder.field(fieldSchema.getName(), fieldType.getType());
            if (fieldSchema.getAlias() != null && !fieldSchema.getAlias().isEmpty()) {
                aliasMap.put(fieldSchema.getAlias(), fieldSchema.getName());
                tableSchemaBuilder.field(fieldSchema.getAlias(), fieldType.getType());
            } else {
                // 如果字段没有别名，就使用原本的名字
                aliasMap.put(fieldSchema.getName(), fieldSchema.getName());
                tableSchemaBuilder.field(fieldSchema.getName(), fieldType.getType());
            }
        }

        Kafka010JsonTableSource.Builder kafkaJsonTableSourceBuilder = Kafka010JsonTableSource.builder();
        kafkaJsonTableSourceBuilder
                // set Kafka topic
                .forTopic(kafkaSourceSchema.getTopic())
                // set Kafka consumer properties
                .withKafkaProperties(kafkaProps)
                // set Table schema
                .withSchema(tableSchemaBuilder.build())
                .forJsonSchema(jsonSchemaBuilder.build())
                .withTableToJsonMapping(aliasMap);

        // flink table source的时间策略类型、字段、水印延迟优先使用job配置文件中source的配置，没有配置的情况下使用kafkaSource配置文件中的默认配置
        Boolean isProcessTimeMode = tableSchema.getProcessTimeMode() == null ? kafkaSourceSchema.getProcessTimeMode() : tableSchema.getProcessTimeMode();
        String strTimeField = (tableSchema.getTimeField() == null || tableSchema.getTimeField().isEmpty()) ?
                kafkaSourceSchema.getTimeField() : tableSchema.getTimeField();
        Long watermarkDelay = tableSchema.getWatermarkDelay() == null ? kafkaSourceSchema.getWatermarkDelay() : tableSchema.getWatermarkDelay();
        if (strTimeField != null && !strTimeField.isEmpty()) {
            if (isProcessTimeMode == Boolean.TRUE) {
                kafkaJsonTableSourceBuilder.withProctimeAttribute(strTimeField);
            } else { // 默认是rowtime模式
                kafkaJsonTableSourceBuilder.withRowtimeAttribute(strTimeField, new ExistingField(strTimeField),
                        new BoundedOutOfOrderTimestamps(watermarkDelay == null ? 0 : watermarkDelay));
            }
        }

        switch (offsetStrategy) {
            case FROM_EARLIEST:
                kafkaJsonTableSourceBuilder.fromEarliest();
                break;
            case FROM_LATEST:
                kafkaJsonTableSourceBuilder.fromLatest();
                break;
            case FROM_GROUP_OFFSET:
            default:
                kafkaJsonTableSourceBuilder.fromGroupOffsets();
                break;
        }

        KafkaTableSource source = kafkaJsonTableSourceBuilder.build();
        // 如果job配置中显示重新指定了source表的名称，则使用指定的；否则使用kafka topic元数据中默认的flink表名称
        String registerName = kafkaSourceSchema.getFlinkTableName();
        if (tableSchema.getName() != null && !tableSchema.getName().isEmpty()) {
            registerName = tableSchema.getName();
        }
        streamTableEnvironment.registerTableSource(registerName, source);
    }



//    /**
//     * 初始化注册表信息，以及注册配置了scan属性的表
//     *
//     * @param tableEnv
//     * @param jobSchema
//     * @throws IOException
//     */
//    public static void registerTableSources(StreamTableEnvironment tableEnv,
//                                            JobSchema jobSchema) throws IOException {
//        Properties properties = GlobalProp.getProperties();
//        String testfilepath = null;
//        testfilepath = jobSchema.getTestFilePath();
//        Map<String, com.caocao.archmage.flink.source.TableSchema> sources = jobSchema.getSources();
//
//        for (Map.Entry<String, com.caocao.archmage.flink.source.TableSchema> entry : sources.entrySet()) {
//            String sourceSchemaFileName = entry.getKey();
//            if (entry.getValue() == null) {
//                throw new RuntimeException("[" + sourceSchemaFileName + "]source没有配置属性");
//            }
//            com.caocao.archmage.flink.source.TableSchema tableSchema = entry.getValue();
//            if (testfilepath == null) {
//                KafkaSourceUtil.registerTableSource(tableEnv,
//                        sourceSchemaFileName, tableSchema, properties);
//            } else {
//                String testfile = testfilepath + java.io.File.separator + sourceSchemaFileName;
//                KafkaSourceUtil.registerTestTableSource(tableEnv,
//                        sourceSchemaFileName, tableSchema, properties, testfile);
//            }
//        }
//    }


}

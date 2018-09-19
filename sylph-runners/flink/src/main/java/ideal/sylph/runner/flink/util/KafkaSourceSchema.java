package ideal.sylph.runner.flink.util;

import java.util.List;

/**
 * kafka数据源配置信息
 */
public class KafkaSourceSchema {
    private String bootstrapServers;
    private String topic;
    private List<FieldSchema> fieldSchemas;
    private Boolean processTimeMode;
    private String timeField;
    private Long watermarkDelay;
    private String flinkTableName;


    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<FieldSchema> getFieldSchemas() {
        return fieldSchemas;
    }

    public void setFieldSchemas(List<FieldSchema> fieldSchemas) {
        this.fieldSchemas = fieldSchemas;
    }

    public Boolean getProcessTimeMode() {
        return processTimeMode;
    }

    public void setProcessTimeMode(Boolean processTimeMode) {
        this.processTimeMode = processTimeMode;
    }

    public String getTimeField() {
        return timeField;
    }

    public void setTimeField(String timeField) {
        this.timeField = timeField;
    }

    public Long getWatermarkDelay() {
        return watermarkDelay;
    }

    public void setWatermarkDelay(Long watermarkDelay) {
        this.watermarkDelay = watermarkDelay;
    }

    public String getFlinkTableName() {
        return flinkTableName;
    }

    public void setFlinkTableName(String flinkTableName) {
        this.flinkTableName = flinkTableName;
    }
}

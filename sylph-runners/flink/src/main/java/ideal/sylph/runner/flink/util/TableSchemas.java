package ideal.sylph.runner.flink.util;

public class TableSchemas {
    private String name;
    private String groupId;
    private String offsetStrategy;
    private Boolean processTimeMode;
    private String timeField;
    private Long watermarkDelay;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getOffsetStrategy() {
        return offsetStrategy;
    }

    public void setOffsetStrategy(String offsetStrategy) {
        this.offsetStrategy = offsetStrategy;
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
}
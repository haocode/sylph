package ideal.sylph.runner.flink.util;

/**
 * kafka消费策略
 */
public enum OffsetStrategy {
    FROM_EARLIEST("earliest"),
    FROM_LATEST("latest"),
    FROM_GROUP_OFFSET("group_offset");

    // 成员变量
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    // 构造方法
    OffsetStrategy(String name) {
        this.name = name;
    }

    public static OffsetStrategy fromName(String name) {
        for (OffsetStrategy strategy : OffsetStrategy.values()) {
            if (strategy.getName().equals(name)) {
                return strategy;
            }
        }
        return null;
    }

}

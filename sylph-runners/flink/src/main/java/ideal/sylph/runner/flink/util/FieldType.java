package ideal.sylph.runner.flink.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public enum FieldType {
    INTEGER("integer", Types.INT),
    TINYINT("tinyint", Types.INT),
    SMALLINT("smallint", Types.INT),
    INT("int", Types.INT),
    LONG("long", Types.LONG),
    BIGINT("bigint", Types.LONG),
    STRING("string", Types.STRING),
    VARCHAR("varchar", Types.STRING),
    VARCHAR2("varchar2", Types.STRING),
    TEXT("text", Types.STRING),
    RAW("raw", Types.BYTE),
    BLOB("blob", Types.BYTE),
    BYTE("byte", Types.BYTE),
    BOOLEAN("boolean", Types.BOOLEAN),
    FLOAT("float", Types.FLOAT),
    DECIMAL("decimal", Types.DOUBLE),
    DOUBLE("double", Types.DOUBLE),
    DATE("date", Types.SQL_DATE),
    TIME("time", Types.SQL_TIME),
    TIMESTAMP("timestamp", Types.SQL_TIMESTAMP),
    ARRAY_STRING("array<string>", Types.LIST(Types.STRING)),
    MAP_STRING("map<string,string>", Types.MAP(Types.STRING, Types.STRING));

    // 成员变量
    private String name;
    private TypeInformation type;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TypeInformation getType() {
        return type;
    }

    public void setType(TypeInformation type) {
        this.type = type;
    }

    // 构造方法
    FieldType(String name, TypeInformation type) {
        this.name = name;
        this.type = type;
    }

    public static FieldType fromTypeName(String typeName) {
        for (FieldType type : FieldType.values()) {
            if (type.getName().equals(typeName)) {
                return type;
            }
        }
        return null;
    }
}
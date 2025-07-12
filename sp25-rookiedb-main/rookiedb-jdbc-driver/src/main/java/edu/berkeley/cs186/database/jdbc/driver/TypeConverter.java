package edu.berkeley.cs186.database.jdbc.driver;

import edu.berkeley.cs186.database.databox.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class TypeConverter {

    // RookieDB 类型到 SQL 类型的映射
    private static final Map<String, Integer> ROOKIEDB_TO_SQL_TYPE = new HashMap<>();
    private static final Map<String, String> ROOKIEDB_TO_JAVA_CLASS = new HashMap<>();
    private static final Map<String, String> ROOKIEDB_TO_SQL_TYPE_NAME = new HashMap<>();

    static {
        // 初始化类型映射
        ROOKIEDB_TO_SQL_TYPE.put("bool", Types.BOOLEAN);
        ROOKIEDB_TO_SQL_TYPE.put("int", Types.INTEGER);
        ROOKIEDB_TO_SQL_TYPE.put("long", Types.BIGINT);
        ROOKIEDB_TO_SQL_TYPE.put("float", Types.REAL);
        ROOKIEDB_TO_SQL_TYPE.put("double", Types.DOUBLE);
        ROOKIEDB_TO_SQL_TYPE.put("string", Types.VARCHAR);

        ROOKIEDB_TO_JAVA_CLASS.put("bool", "java.lang.Boolean");
        ROOKIEDB_TO_JAVA_CLASS.put("int", "java.lang.Integer");
        ROOKIEDB_TO_JAVA_CLASS.put("long", "java.lang.Long");
        ROOKIEDB_TO_JAVA_CLASS.put("float", "java.lang.Float");
        ROOKIEDB_TO_JAVA_CLASS.put("double", "java.lang.Double");
        ROOKIEDB_TO_JAVA_CLASS.put("string", "java.lang.String");

        ROOKIEDB_TO_SQL_TYPE_NAME.put("bool", "BOOLEAN");
        ROOKIEDB_TO_SQL_TYPE_NAME.put("int", "INTEGER");
        ROOKIEDB_TO_SQL_TYPE_NAME.put("long", "BIGINT");
        ROOKIEDB_TO_SQL_TYPE_NAME.put("float", "REAL");
        ROOKIEDB_TO_SQL_TYPE_NAME.put("double", "DOUBLE");
        ROOKIEDB_TO_SQL_TYPE_NAME.put("string", "VARCHAR");
    }

    // DataBox 到各种 Java 类型的转换方法
    public static String dataBoxToString(DataBox dataBox) {
        if (dataBox == null) return null;
        return dataBox.toString();
    }

    public static boolean dataBoxToBoolean(DataBox dataBox) {
        if (dataBox == null) return false;
        if (dataBox instanceof BoolDataBox) {
            return ((BoolDataBox) dataBox).getBool();
        }
        // 尝试从其他类型转换
        String str = dataBox.toString().toLowerCase();
        return "true".equals(str) || "1".equals(str);
    }

    public static byte dataBoxToByte(DataBox dataBox) {
        if (dataBox == null) return 0;
        if (dataBox instanceof IntDataBox) {
            return (byte) ((IntDataBox) dataBox).getInt();
        }
        return Byte.parseByte(dataBox.toString());
    }

    public static short dataBoxToShort(DataBox dataBox) {
        if (dataBox == null) return 0;
        if (dataBox instanceof IntDataBox) {
            return (short) ((IntDataBox) dataBox).getInt();
        }
        return Short.parseShort(dataBox.toString());
    }

    public static int dataBoxToInt(DataBox dataBox) {
        if (dataBox == null) return 0;
        if (dataBox instanceof IntDataBox) {
            return ((IntDataBox) dataBox).getInt();
        }
        if (dataBox instanceof LongDataBox) {
            return (int) ((LongDataBox) dataBox).getLong();
        }
        return Integer.parseInt(dataBox.toString());
    }

    public static long dataBoxToLong(DataBox dataBox) {
        if (dataBox == null) return 0L;
        if (dataBox instanceof LongDataBox) {
            return ((LongDataBox) dataBox).getLong();
        }
        if (dataBox instanceof IntDataBox) {
            return ((IntDataBox) dataBox).getInt();
        }
        return Long.parseLong(dataBox.toString());
    }

    public static float dataBoxToFloat(DataBox dataBox) {
        if (dataBox == null) return 0.0f;
        if (dataBox instanceof FloatDataBox) {
            return ((FloatDataBox) dataBox).getFloat();
        }
//        if (dataBox instanceof DoubleDataBox) {
//            return (float) ((DoubleDataBox) dataBox).getDouble();
//        }
        return Float.parseFloat(dataBox.toString());
    }

    public static double dataBoxToDouble(DataBox dataBox) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
//        if (dataBox == null) return 0.0;
//        if (dataBox instanceof DoubleDataBox) {
//            return ((DoubleDataBox) dataBox).getDouble();
//        }
//        if (dataBox instanceof FloatDataBox) {
//            return ((FloatDataBox) dataBox).getFloat();
//        }
//        return Double.parseDouble(dataBox.toString());
    }

    public static BigDecimal dataBoxToBigDecimal(DataBox dataBox, int scale) {
        if (dataBox == null) return null;
        BigDecimal bd = new BigDecimal(dataBox.toString());
        return bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
    }

    public static byte[] dataBoxToBytes(DataBox dataBox) {
        if (dataBox == null) return null;
        if (dataBox instanceof StringDataBox) {
            return ((StringDataBox) dataBox).getString().getBytes();
        }
        return dataBox.toString().getBytes();
    }

    public static Date dataBoxToDate(DataBox dataBox) {
        if (dataBox == null) return null;
        // 假设日期以字符串形式存储
        return Date.valueOf(dataBox.toString());
    }

    public static Time dataBoxToTime(DataBox dataBox) {
        if (dataBox == null) return null;
        // 假设时间以字符串形式存储
        return Time.valueOf(dataBox.toString());
    }

    public static Timestamp dataBoxToTimestamp(DataBox dataBox) {
        if (dataBox == null) return null;
        // 假设时间戳以字符串形式存储
        return Timestamp.valueOf(dataBox.toString());
    }

    public static Object dataBoxToObject(DataBox dataBox) {
        if (dataBox == null) return null;

        if (dataBox instanceof BoolDataBox) {
            return ((BoolDataBox) dataBox).getBool();
        } else if (dataBox instanceof IntDataBox) {
            return ((IntDataBox) dataBox).getInt();
        } else if (dataBox instanceof LongDataBox) {
            return ((LongDataBox) dataBox).getLong();
        } else if (dataBox instanceof FloatDataBox) {
            return ((FloatDataBox) dataBox).getFloat();
        }
//        else if (dataBox instanceof DoubleDataBox) {
//            return ((DoubleDataBox) dataBox).getDouble();
//        }
        else if (dataBox instanceof StringDataBox) {
            return ((StringDataBox) dataBox).getString();
        }

        return dataBox.toString();
    }

    // 类型映射方法
    public static int rookieDBTypeToSQLType(String rookieDBType) {
        return ROOKIEDB_TO_SQL_TYPE.getOrDefault(rookieDBType.toLowerCase(), Types.OTHER);
    }

    public static String rookieDBTypeToJavaClassName(String rookieDBType) {
        return ROOKIEDB_TO_JAVA_CLASS.getOrDefault(rookieDBType.toLowerCase(), "java.lang.Object");
    }

    public static int rookieTypeToSqlType(Type type) {
        String typeName = getTypeNameFromType(type);
        return rookieDBTypeToSQLType(typeName);
    }

    public static String rookieTypeToSqlTypeName(Type type) {
        String typeName = getTypeNameFromType(type);
        return ROOKIEDB_TO_SQL_TYPE_NAME.getOrDefault(typeName.toLowerCase(), "OTHER");
    }

    public static String rookieTypeToJavaClassName(Type type) {
        String typeName = getTypeNameFromType(type);
        return rookieDBTypeToJavaClassName(typeName);
    }

    // 辅助方法：从 Type 对象获取类型名称
    private static String getTypeNameFromType(Type type) {
        if (type == Type.boolType()) {
            return "bool";
        } else if (type == Type.intType()) {
            return "int";
        } else if (type == Type.longType()) {
            return "long";
        } else if (type == Type.floatType()) {
            return "float";
        } else if (type.getClass().getSimpleName().toLowerCase().contains("string")) {
            return "string";
        } else {
            return "string"; // 默认为字符串类型
        }
    }

    // Java 类型到 DataBox 的转换方法（用于 PreparedStatement）
    public static DataBox javaObjectToDataBox(Object value, String targetType) {
        if (value == null) {
            return null;
        }

        switch (targetType.toLowerCase()) {
            case "bool":
                if (value instanceof Boolean) {
                    return new BoolDataBox((Boolean) value);
                }
                return new BoolDataBox(Boolean.parseBoolean(value.toString()));

            case "int":
                if (value instanceof Integer) {
                    return new IntDataBox((Integer) value);
                }
                return new IntDataBox(Integer.parseInt(value.toString()));

            case "long":
                if (value instanceof Long) {
                    return new LongDataBox((Long) value);
                }
                return new LongDataBox(Long.parseLong(value.toString()));

            case "float":
                if (value instanceof Float) {
                    return new FloatDataBox((Float) value);
                }
                return new FloatDataBox(Float.parseFloat(value.toString()));

//            case "double":
//                if (value instanceof Double) {
//                    return new DoubleDataBox((Double) value);
//                }
//                return new DoubleDataBox(Double.parseDouble(value.toString()));

            case "string":
                return new StringDataBox(value.toString(), value.toString().length());

            default:
                return new StringDataBox(value.toString(), value.toString().length());
        }
    }

    public static <T> T convertToType(Object object, Class<T> type) throws RookieDBSQLException {
        if (object == null) {
            return null;
        }

        // 检查类型兼容性
        if (type.isAssignableFrom(object.getClass())) {
            return type.cast(object);
        }

        // 处理基本类型转换
        try {
            if (type == String.class) {
                return type.cast(object.toString());
            } else if (type == Integer.class || type == int.class) {
                if (object instanceof Number) {
                    return type.cast(((Number) object).intValue());
                }
                return type.cast(Integer.parseInt(object.toString()));
            } else if (type == Long.class || type == long.class) {
                if (object instanceof Number) {
                    return type.cast(((Number) object).longValue());
                }
                return type.cast(Long.parseLong(object.toString()));
            } else if (type == Float.class || type == float.class) {
                if (object instanceof Number) {
                    return type.cast(((Number) object).floatValue());
                }
                return type.cast(Float.parseFloat(object.toString()));
            } else if (type == Boolean.class || type == boolean.class) {
                if (object instanceof Boolean) {
                    return type.cast(object);
                }
                return type.cast(Boolean.parseBoolean(object.toString()));
            }

            // 如果无法转换，抛出异常
            throw new RookieDBSQLException(
                    "Cannot convert " + object.getClass().getSimpleName() +
                            " to " + type.getSimpleName(), "22018");

        } catch (NumberFormatException e) {
            throw new RookieDBSQLException(
                    "Invalid number format: " + object.toString(), "22018");
        }
    }
}

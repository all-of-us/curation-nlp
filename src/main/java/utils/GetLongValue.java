package utils;

public class GetLongValue {
    public static Long of(String strVal) {
        try {
            return Long.valueOf(strVal);
        } catch (Exception e) {
            return 0L;
        }
    }
}
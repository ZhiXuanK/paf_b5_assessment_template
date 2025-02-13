package vttp.batch5.paf.movies;

import org.springframework.stereotype.Component;

import jakarta.json.JsonObject;

@Component
public class Utils {

    public String checkString(JsonObject obj, String field) {
        try {
            String value = obj.getString(field);
            return value;
        } catch (Exception e) {
            return "";
        }
    }

    public int checkInt(JsonObject obj, String field) {
        try {
            int value = obj.getInt(field);
            return value;
        } catch (Exception e) {
            return 0;
        }
    }

    public float checkFloat(JsonObject obj, String field) {
        try {
            float value =(float)obj.getJsonNumber(field).doubleValue();
            return value;
        } catch (Exception e) {
            return 0f;
        }
    }

    public long checkLong(JsonObject obj, String field){
        try {
            long value = (long) obj.getJsonNumber(field).longValue();
            return value;
        } catch (Exception e) {
            return 0L;
        }
    }

}

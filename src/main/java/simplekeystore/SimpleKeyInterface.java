package simplekeystore;

import java.io.IOException;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import simplekeystore.implementation.CustomizedException;

public interface SimpleKeyInterface {

    void create(String key, JSONObject value) throws CustomizedException, IOException;

    void create(String key, JSONObject value, long timeToLive) throws IOException, CustomizedException;

    JSONObject read(String key) throws CustomizedException, IOException, ParseException;

    void delete(String Key) throws CustomizedException, IOException;
}

package simplekeystore;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import simplekeystore.implementation.CustomizedException;
import simplekeystore.implementation.KeyStore;

import java.io.IOException;

public class Demo {
    private static SimpleKeyInterface keystore;

    public static void main(String[] args) throws IOException, CustomizedException, ParseException {
        keystore=new KeyStore();
        String key;

        for(int i=1; i<1000; i++) {

            JSONObject json = new JSONObject();
            json.put("name", "foo");
            json.put("num", i);
            json.put("balance", i*10);
            json.put("is_vip", true);

            key = "key".concat(String.valueOf(i));
            keystore.create(key, json);
        }

        System.out.println(keystore.read("key569"));
    }
}

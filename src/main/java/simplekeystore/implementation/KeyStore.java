package simplekeystore.implementation;

import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import simplekeystore.implementation.*;
import simplekeystore.SimpleKeyInterface;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;


public class KeyStore implements SimpleKeyInterface {

    private ConcurrentHashMap<String, MetaData> mapKeyAndValueLoc = new ConcurrentHashMap<>();
    public PriorityQueue<MetaData> deletedSpacesInFile = new PriorityQueue<>();
    private RandomAccessFile valuesFile;
    private static final int MAX_KEY_LENGTH = 32;
    private static final int MAX_VALUE_SIZE = 16*1024;
    private int NUMBER_OF_OPERATIONS = 0;

    public KeyStore(String fileName) throws FileNotFoundException{
            valuesFile = new RandomAccessFile(new File(fileName), "rwd");
    }

    public KeyStore() throws FileNotFoundException{
        valuesFile = new RandomAccessFile(new File("src/main/resources/valuesFile"+Math.random()+".txt"), "rwd");
    }

    @Override
    public void create(String key, JSONObject value) throws IOException, CustomizedException {
        writeToFile(key, value.toJSONString(), 0, false);
    }

    @Override
    public void create(String key, JSONObject value, long timeToLive) throws IOException, CustomizedException {
        writeToFile(key, value.toJSONString(), timeToLive, true);
    }

    private synchronized void writeToFile(String key, String value, long timeToLive, boolean removeAfterExpiry) throws CustomizedException, IOException {
        long writeFrom;
        checkSizeConstraints(key, value);
        checkKeyExists(key);
        Integer valueLength = value.length();

        if (deletedSpacesInFile.size() > 0){
            writeFrom = getEmptySpaceLocation(valueLength);
            valuesFile.seek(writeFrom);
        }
        else{
            valuesFile.seek(valuesFile.length());
        }

        insertIntoHashMap(key, valueLength, valuesFile.length(), timeToLive, removeAfterExpiry);
        valuesFile.writeUTF(value);
        NUMBER_OF_OPERATIONS++;
        if (NUMBER_OF_OPERATIONS > 1000){
            removeExpiredKeys();
            NUMBER_OF_OPERATIONS = 0;
        }
    }

    private long getEmptySpaceLocation(Integer valueLength){
        Iterator queueIterator = deletedSpacesInFile.iterator();
        MetaData nodeMeta = null;

        while(queueIterator.hasNext()){
            nodeMeta = (MetaData) queueIterator.next();
            if(nodeMeta.length >= valueLength){
                deletedSpacesInFile.remove(nodeMeta);
                break;
            }
        }
        return nodeMeta.offset;
    }

    private void insertIntoHashMap(String key, Integer valueLength, long offset, long timeToLive, boolean removeAfterExpiry) {
        LocalTime time = LocalTime.now();
        MetaData newNodeMeta = new MetaData();
        newNodeMeta.length = valueLength;
        newNodeMeta.offset = offset;
        newNodeMeta.expiresAt = time.plusSeconds(timeToLive);
        newNodeMeta.removeAfterExpiry = removeAfterExpiry;
        mapKeyAndValueLoc.put(key,newNodeMeta);
    }

    @Override
    public synchronized JSONObject read(String key) throws IOException, NullPointerException, ParseException, CustomizedException {
        if (!mapKeyAndValueLoc.contains(key)) throw new CustomizedException("Key does not exist");
        String value;
        MetaData meta = mapKeyAndValueLoc.get(key);
        valuesFile.seek(meta.offset);
        value = valuesFile.readUTF();
        JSONParser parser = new JSONParser();
        JSONObject json = (JSONObject) parser.parse(value);
        NUMBER_OF_OPERATIONS++;
        return json;
    }

    @Override
    public synchronized void delete(String key) throws CustomizedException {
        //if (!mapKeyAndValueLoc.contains(key)) throw new CustomizedException("Key does not exist");
        MetaData metaOfDeleted = mapKeyAndValueLoc.get(key);
        mapKeyAndValueLoc.remove(key);
        deletedSpacesInFile.add(metaOfDeleted);
        NUMBER_OF_OPERATIONS++;
    }

    private void checkSizeConstraints(String key, String value) throws CustomizedException {
        if (key.length() > MAX_KEY_LENGTH) throw new CustomizedException("Length of Key exceeds limit");
        if (MAX_VALUE_SIZE < value.getBytes().length) throw new CustomizedException("Size of Value exceeds limit");
        if (key == null) throw new CustomizedException("Key cannot be null");
        if (value == null) throw new CustomizedException("Value cannot be null");
    }

    private void checkKeyExists(String key) throws CustomizedException {
        if (mapKeyAndValueLoc.containsKey(key)) throw new CustomizedException("Key already Exists");
    }

    private void removeExpiredKeys() throws CustomizedException {
        MetaData metaData;
        for(String key: mapKeyAndValueLoc.keySet()){
            metaData = mapKeyAndValueLoc.get(key);
            LocalTime timeNow = LocalTime.now();
            if (timeNow.getSecond() >= metaData.expiresAt.getSecond() && metaData.removeAfterExpiry){
                delete(key);
                System.out.println(key + " Expired");
            }
        }
    }
}





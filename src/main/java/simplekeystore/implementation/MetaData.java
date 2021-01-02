package simplekeystore.implementation;

import java.time.LocalTime;

class MetaData implements Comparable<MetaData>{
    int length;
    long offset;
    LocalTime expiresAt;
    boolean removeAfterExpiry;

    public int getLength() {
        return length;
    }

    @Override
    public int compareTo(MetaData metaData) {
        return this.getLength()>=metaData.getLength()?1:-1;
    }
}

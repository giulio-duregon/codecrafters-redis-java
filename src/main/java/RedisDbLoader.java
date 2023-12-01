import resp.RespBulkString;
import resp.RespData;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static resp.RespConstants.*;

public class RedisDbLoader {
    private final ByteBuffer buffer;
    private final ConcurrentHashMap<RespData, RespData> db;
    private final ConcurrentHashMap<RespData, Instant> keyExp;
    private final Logger logger = Logger.getLogger(RedisDbLoader.class.toString());

    public RedisDbLoader(File filepath, ConcurrentHashMap<RespData, RespData> db, ConcurrentHashMap<RespData, Instant> keyExp) throws IOException {
        this.buffer = ByteBuffer.wrap(new FileInputStream(filepath).readAllBytes());
        this.db = db;
        this.keyExp = keyExp;
    }

    public void init() throws IOException {
        readPreamble();
        loadDb();
    }

    private void readPreamble() {
        String redis = readBytesToString(5);
        String version = readBytesToString(4);
        logger.info("Loaded Redis Magic String=%s, Version=%s".formatted(redis, version));
    }

    private void loadDb() throws IOException {
        boolean finished = false;
        while (!finished) {
            byte b = readByte();
            switch (b) {
                case EOF -> finished = true;
                case SELECTDB -> loadDbNumber();
                case AUX -> loadAuxArg();
                case EXPIRETIME -> loadKeyWithExp(true);
                case EXPIRETIMEMS -> loadKeyWithExp(false);
                case RESIZEDB -> loadDbSizes();
                default -> loadKeyAndPut(b);
            }
        }
        logger.info("Finished loading db file, DB size=%d, Expiry DB size=%d".formatted(db.size(), keyExp.size()));
    }

    private void loadAuxArg() throws IOException {
        String key = valueDecode(readByte());
        String value = valueDecode(readByte());
        logger.info("Loaded AUX Argument, Key=%s : Value=%s".formatted(key, value));
    }

    private void loadDbNumber() throws IOException {
        int db_num = lengthDecode(readByte());
        logger.info("Loaded Redis DB Number=%d".formatted(db_num));
    }

    private void loadDbSizes() throws IOException {
        int hashTableSize = lengthDecode(readByte());
        int expiryHashTableSize = lengthDecode(readByte());
        logger.info("Loaded Hashtable Size=%d, expiryHashTableSize=%d".formatted(hashTableSize, expiryHashTableSize));
    }

    private void loadKeyAndPut(byte b) throws IOException {
        Map.Entry<RespBulkString, RespBulkString> keyValuePair = loadKey(b);
        this.db.put(keyValuePair.getKey(), keyValuePair.getValue());
    }

    private Map.Entry<RespBulkString, RespBulkString> loadKey(byte b) throws IOException {
        //Value type
        RespBulkString keyString = readRespBulkString();
        checkValueType(b);
        RespBulkString valueString = readRespBulkString();
        return Map.entry(keyString, valueString);
    }

    private Instant parseKeyTime(boolean inSeconds) {
        Instant expTimeMillis = null;
        if (inSeconds) {
            expTimeMillis = Instant.ofEpochSecond(Integer.toUnsignedLong(readInt()));
        } else {
            expTimeMillis = Instant.ofEpochMilli(readLong());
        }
        return expTimeMillis;
    }

    private void putKeyIfNotExpired(Map.Entry<RespBulkString, RespBulkString> kvPair, Instant expTime) {
        // If key not expired
        if (expTime.isAfter(Instant.now())) {
            this.db.put(kvPair.getKey(), kvPair.getValue());
            this.keyExp.put(kvPair.getKey(), expTime);
        }
    }

    private void checkValueType(byte b) {
        if (b != 0) {
            logger.severe("WARNING, VALUE TYPE=%d NOT SUPPORTED YET%n".formatted(b));
        }
    }

    private void loadKeyWithExp(boolean inSeconds) throws IOException {
        // "expiry time in seconds", followed by 4 byte unsigned int
        Instant expTimeMillis = parseKeyTime(inSeconds);
        logger.info("Parsed key expiration time %s".formatted(expTimeMillis));

        // Load key value pair
        Map.Entry<RespBulkString, RespBulkString> keyValuePair = loadKey(readByte());

        // Insert it into both DB and Key Expiry DB if not expired
        putKeyIfNotExpired(keyValuePair, expTimeMillis);
    }

    private RespBulkString readRespBulkString() throws IOException {
        return new RespBulkString(valueDecode(readByte()));
    }

    private byte[] readByteArray(int size) {
        byte[] byteArr = new byte[size];
        buffer.get(byteArr);
        return byteArr;
    }

    private String readBytesToString(int size) {
        return new String(readByteArray(size), StandardCharsets.UTF_8);
    }

    private byte readByte() {
        return buffer.get();
    }

    private int readShort() {
        return buffer.getShort();
    }

    private int readInt() {
        return buffer.getInt();
    }

    private long readLong() {
        return buffer.getLong();
    }

    private String valueDecode(byte b) throws IOException {
        int len = lengthDecode(b);
        if (len == -1) {
            return specialDecode(b);
        }
        return readBytesToString(len);
    }

    private int lengthDecode(byte b) throws IOException {
        // Read first 2 most significant bits
        int msb = ((b & 0xC0) >> 6);
        int lsb = (b & 0x3F);
        int len = 0;
        switch (msb) {
            // 00 -> The next 6 bits represent the length
            case 0 -> len = lsb;
            // 01 -> The 6 bits are added with the next byte
            case 1 -> len = (lsb << 8) | readByte();
            // 10 -> The next 4 bytes represent the length
            case 2 -> len = readInt();
            // Special encoding necessary
            case 3 -> len = -1;
        }
        return len;
    }

    private String specialDecode(byte b) {
        int lsb = (b & 0x3F);
        String value = null;
        switch (lsb) {
            case 0 -> value = String.valueOf(readByte());
            case 1 -> value = String.valueOf(readShort());
            case 2 -> value = String.valueOf(readInt());
        }
        return value;
    }
}

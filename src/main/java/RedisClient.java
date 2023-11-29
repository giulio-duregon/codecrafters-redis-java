import resp.*;

import java.io.*;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static resp.RedisCommands.*;
import static resp.RespConstants.*;

public class RedisClient implements Runnable {
    private final Logger logger = Logger.getLogger(RedisClient.class + "-Thread-" + Thread.currentThread().getName());
    private final Socket socket;
    private final BufferedReader in;
    private final BufferedWriter out;

    private final ConcurrentHashMap<RespData, RespData> db;
    private final ConcurrentHashMap<RespData, Instant> keyExpiry;

    public RedisClient(Socket socket,
                       ConcurrentHashMap<RespData, RespData> db,
                       ConcurrentHashMap<RespData, Instant> keyExpiry) throws IOException {
        this.socket = socket;
        this.in = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
        this.out = new BufferedWriter(new OutputStreamWriter(this.socket.getOutputStream()));
        this.db = db;
        this.keyExpiry = keyExpiry;
    }

    private RedisCommands parseCommand(String data) {
        String commandString = data.toLowerCase();
        RedisCommands command;
        if (commandString.equals("ping")) {
            command = PING;
        } else if (commandString.equals("echo")) {
            command = ECHO;
        } else if (commandString.equals("get")) {
            command = GET;
        } else if (commandString.equals("set")) {
            command = SET;
        } else {
            command = INVALID;
        }
        logger.log(Level.INFO, "Parsed String: %s Command: %s".formatted(commandString, command));
        return command;
    }

    private void write(RespData data) throws IOException {
        out.write(data.toRespString());
        out.flush();
    }

    private void handlePing() throws IOException {
        write(PONG);
    }

    private void handleEcho(RespData response) throws IOException {
        write(response);
    }

    private boolean keyExpired(RespData key) {
        Instant value = this.keyExpiry.get(key);
        if (value != null || value.isBefore(Instant.now())) {
            this.db.remove(key);
            this.keyExpiry.remove(key);
            return true;
        }
        return false;
    }

    private void handleGet(RespData key) throws IOException {
        logger.log(Level.INFO, "Handling GET operation for key %s".formatted(key.toString()));
        RespData value = db.get(key);
        if (keyExpired(key) || value == null) {
            value = NULLBULKRESP;
        }
        write(value);
    }

    private boolean moreArgsPresent(RespArray commandArray) {
        return commandArray.dataArray().size() >= 2;
    }

    private Instant getKeyExpireTime(RespArray commandArr, String unit) {
        // Parse the arguments to determine unit of time for key expiry

        // MilliSeconds
        if (unit.equals("px")) {
            return Instant.ofEpochMilli(Integer.parseUnsignedInt(commandArr.popFront().inputString()));
            // Seconds
        } else if (unit.equals("ex")) {
            return Instant.ofEpochSecond(Integer.parseUnsignedInt(commandArr.popFront().inputString()));
        }
        // Unsure of difference between PX / EXAT args
        return Instant.ofEpochMilli(Integer.parseUnsignedInt(commandArr.popFront().inputString()));
    }

    private void Set(RespData key, RespData value) throws IOException {
        this.db.put(key, value);
        write(OK);
    }

    private void Set(RespData key, RespData value, Instant expTime) throws IOException {
        this.db.put(key, value);
        this.keyExpiry.put(key, expTime);
        write(OK);
    }

    private void handleSet(RespArray commandArray) throws IOException {
        // Grab the KV pair from args passed
        RespData key = commandArray.popFront();
        RespData value = commandArray.popFront();
        logger.log(Level.INFO, "Handling SET operation for key %s, Value %s".formatted(key.toString(), value.toString()));

        // Check if there are additional optional arguments for time expiry
        if (moreArgsPresent(commandArray)) {
            String unit = commandArray.popFront().inputString();
            Instant expTime = getKeyExpireTime(commandArray, unit.toLowerCase()).plusMillis(Instant.now().toEpochMilli());
            logger.log(Level.INFO, "Adding key %s value %s with expiry %d".formatted(key.toString(), value.toString(), expTime.toEpochMilli()));
            Set(key, value, expTime);
            return;
        }
        logger.log(Level.INFO, "Adding key %s value %s without expiry".formatted(key.toString(), value.toString()));
        Set(key, value);
    }

    private void handleCommand(RedisCommands command, RespArray commandArray) throws IOException {
        switch (command) {
            case PING -> handlePing();
            case ECHO -> handleEcho(commandArray.popFront());
            case SET -> handleSet(commandArray);
            case GET -> handleGet(commandArray.popFront());
        }
    }

    private RespArray parseRespArray(int size) throws IOException {
        ArrayList<RespData> arr = new ArrayList<>();
        String inputLine;
        for (int i = 0; i < size; i++) {
            inputLine = in.readLine();
            arr.add(parse(inputLine));
        }
        return new RespArray(arr);
    }

    private RespData parse(String s) throws IOException {
        RespData request;
        switch (s.charAt(0)) {
            // Resp Simple String
            case '+' -> request = new RespSimpleString(s.substring(1, s.length() - 2));
            // Resp Bulk String
            case '$' -> request = new RespBulkString(in.readLine());
            case '*' -> request = parseRespArray(Character.getNumericValue(s.charAt(1)));
            default -> request = NULLBULKRESP;
        }
        return request;
    }

    public void run() {
        try {
            String inputLine;
            RedisCommands command;
            while ((inputLine = in.readLine()) != null) {
                if (inputLine.isEmpty()) {
                    continue;
                }
                RespArray commandArray = (RespArray) parse(inputLine);
                RespBulkString rawCommand = commandArray.popFront();
                command = parseCommand(rawCommand.inputString());
                handleCommand(command, commandArray);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed during run in Thread "
                    + Thread.currentThread().getName() +
                    ", exception=" + e.getMessage());
        }
        logger.log(Level.INFO, "Finished execution");
        close();
    }

    private void close() {
        try {
            in.close();
            out.close();
        } catch (IOException e) {
            logger.log(Level.SEVERE,
                    "Failing to close input and output streams in thread: " + Thread.currentThread().getName());
        }
    }
}

import resp.*;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import static resp.RedisCommands.*;
import static resp.RespConstants.NULLBULKRESP;
import static resp.RespConstants.PONG;

public class RedisClient implements Runnable{
    private final Logger logger = Logger.getLogger(RedisClient.class + "-Thread-" + Thread.currentThread().getName());
    private final Socket socket;
    private final BufferedReader in;
    private final BufferedWriter out;
    public RedisClient(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedReader(new InputStreamReader(this.socket.getInputStream()));
        this.out = new BufferedWriter(new OutputStreamWriter(this.socket.getOutputStream()));
    }

    private RedisCommands parseCommand(String data){
        String commandString = data.toLowerCase();
        RedisCommands command;
        if (commandString.equals("ping")){
            command = PING;
        }
        else if (commandString.equals("echo")){
            command = ECHO;
        }
        else {command = INVALID;}
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

    private void handleEcho(RespData response) throws IOException{
        write(response);
    }
    private void handleCommand(RedisCommands command, RespArray commandArray) throws IOException {
    switch (command){
        case PING -> handlePing();
        case ECHO -> handleEcho(commandArray.popFront());
        }
    }

    private RespArray parseRespArray(int size) throws IOException {
        ArrayList<RespData> arr = new ArrayList<>();
        String inputLine;
        for(int i =0; i < size; i++){
            inputLine = in.readLine();
            arr.add(parse(inputLine));
        }
        return new RespArray(arr);
    }

    private RespData parse(String s) throws IOException{
        RespData request;
        switch (s.charAt(0)){
            // Resp Simple String
            case '+' -> request = new RespSimpleString(s.substring(1,s.length()-2));
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
            int i = 0;
            while ((inputLine = in.readLine()) != null){
                if (inputLine.isEmpty()){
                    continue;
                }
                    RespArray commandArray = (RespArray) parse(inputLine);
                    RespBulkString rawCommand = commandArray.popFront();
                    command = parseCommand(rawCommand.inputString());
                    handleCommand(command, commandArray);
                }
            } catch (IOException e){
                logger.log(Level.SEVERE, "Failed during run in Thread "
                        + Thread.currentThread().getName()  +
                        ", exception=" + e.getMessage());
            }
            logger.log(Level.INFO, "Finished execution");
            close();
    }

    private void close(){
        try {
        in.close();
        out.close();}
        catch (IOException e){
            logger.log(Level.SEVERE,
                    "Failing to close input and output streams in thread: " + Thread.currentThread().getName());
        }
    }
}

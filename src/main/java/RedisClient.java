import resp.RedisCommands;
import resp.RespBulkString;
import resp.RespData;

import java.io.*;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static resp.RedisCommands.*;
import static resp.RespConstants.CRLF;
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
        String commandString = data;
        RedisCommands command;
        switch (commandString.toLowerCase()){
            case "ping" -> command = PING;
            case "echo" -> command = ECHO;
            default -> command = INVALD;
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

    private void handleEcho() throws IOException{
        RespBulkString input = new RespBulkString(in.readLine());
        write(input);
    }
    private void handleCommand(RedisCommands command) throws IOException {
    switch (command){
        case PING -> handlePing();
        case ECHO -> handleEcho();
    }
    }
    public void run() {
        try {
            String inputLine;
            RedisCommands command;
            while ((inputLine = in.readLine()) != null){
                    command = parseCommand(inputLine);
                    handleCommand(command);
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

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class Main {
  public static void main(String[] args){
      Logger logger = Logger.getLogger(Main.class.toString());
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    ExecutorService executorService = Executors.newCachedThreadPool();

    final String CRLF = "\r\n";
    //  Uncomment this block to pass the first stage
        ServerSocket serverSocket;
        int port = 6379;
        try {
          serverSocket = new ServerSocket(port);
          serverSocket.setReuseAddress(true);
          while (true){
              // Wait for connection from client.
              Socket clientSocket = serverSocket.accept();
              executorService.execute(() -> {
                  try {
                      new RedisClient(clientSocket);
                  } catch (IOException e) {
                      throw new RuntimeException(e);
                  }
              });
          }

        } catch (IOException e) {
            logger.throwing(Main.class.toString(), "main", new IOException());
          System.out.println("IOException: " + e.getMessage());
        }
  }
}

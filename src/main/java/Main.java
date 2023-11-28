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
        Socket clientSocket = null;
        int port = 6379;
        try {
          serverSocket = new ServerSocket(port);
          serverSocket.setReuseAddress(true);
          while (true){
              // Wait for connection from client.
              clientSocket = serverSocket.accept();
              BufferedReader in = new BufferedReader( new InputStreamReader(clientSocket.getInputStream()));
              BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
              executorService.execute(() -> {
                  try {
                  String inputLine;
                  while ((inputLine = in.readLine()) != null){
                      if (inputLine.toLowerCase().contains("ping")){
                          logger.info(Thread.currentThread().getName());
                          out.write("+PONG"+CRLF);
                          out.flush();
                      }
                  }}
                  catch (IOException e){
                      System.out.println("Exception caught in thread:" + Thread.currentThread().getName() + ", exception: " + e.getMessage());
                  }
              });
          }

        } catch (IOException e) {
            logger.throwing(Main.class.toString(), "main", new IOException());
          System.out.println("IOException: " + e.getMessage());
        } finally {
          try {
            if (clientSocket != null) {
              clientSocket.close();
            }
          } catch (IOException e) {
              logger.throwing(Main.class.toString(), "main", new IOException());
            System.out.println("IOException: " + e.getMessage());
          }
        }
  }
}

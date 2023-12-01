import resp.RdbConfig;
import resp.RespData;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class Main {
    public static void main(String[] args) {
        // Initialize logger
        Logger logger = Logger.getLogger(Main.class.toString());
        // Initialize variables to be shared across all clients
        ServerSocket serverSocket;
        int port = 6379;
        ExecutorService executorService = Executors.newCachedThreadPool();
        ConcurrentHashMap<RespData, RespData> db = new ConcurrentHashMap<>();
        ConcurrentHashMap<RespData, Instant> keyExpiry = new ConcurrentHashMap<>();

        // Parse config
        Optional<RdbConfig> config = RbfConfigParser.parseConfig(args);

        try {
            // Bind to port
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            // Accept connections from client and have execturo service handle
            while (true) {
                // Wait for connection from client.
                Socket clientSocket = serverSocket.accept();
                executorService.execute(() -> {
                    try {
                        new RedisClient(clientSocket, db, keyExpiry, config).run();
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

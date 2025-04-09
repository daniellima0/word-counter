
// Reducer.java
import java.io.*;
import java.net.*;
import java.util.*;

public class Reducer {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: Reducer <CoordinatorAddress>");
            return;
        }

        String coordinatorAddress = args[0];

        try (ServerSocket serverSocket = new ServerSocket(8888)) {
            System.out.println("Reducer waiting for connection...");
            try (Socket socket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                Map<String, Integer> aggregatedResults = new HashMap<>();
                String line;
                while ((line = in.readLine()) != null) {
                    String[] parts = line.split(":");
                    String word = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    aggregatedResults.put(word, aggregatedResults.getOrDefault(word, 0) + count);
                }

                // Output the aggregated result
                for (Map.Entry<String, Integer> entry : aggregatedResults.entrySet()) {
                    System.out.println(entry.getKey() + ": " + entry.getValue());
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

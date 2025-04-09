
// Coordinator.java
import java.io.*;
import java.net.*;
import java.util.*;

public class Coordinator {
    private static final int MAP_PORT = 3000;
    private static final int REDUCE_PORT = 5000;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: Coordinator <numberOfMaps> <numberOfReduces>");
            return;
        }

        int numberOfMaps = Integer.parseInt(args[0]);
        int numberOfReduces = Integer.parseInt(args[1]);

        // Store word counts received from Mappers
        Map<String, Integer> wordCounts = new HashMap<>();

        try (ServerSocket mapServerSocket = new ServerSocket(MAP_PORT);
                ServerSocket reduceServerSocket = new ServerSocket(REDUCE_PORT)) {

            // Accept and assign tasks to Map nodes
            for (int i = 0; i < numberOfMaps; i++) {
                System.out.println("Coordinator: Waiting for Mapper " + i);
                Socket mapSocket = mapServerSocket.accept();
                // Handle communication with Map nodes here
                System.out.println("Coordinator: Mapper " + i + " connected");

                // Read data sent by Mapper (word count results)
                BufferedReader in = new BufferedReader(new InputStreamReader(mapSocket.getInputStream()));
                String line;
                while ((line = in.readLine()) != null) {
                    String[] parts = line.split(":");
                    if (parts.length == 2) {
                        String word = parts[0];
                        int count = Integer.parseInt(parts[1]);
                        wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
                    }
                }

                mapSocket.close();
            }

            // Print aggregated word counts received from all Mappers
            System.out.println("Coordinator: Aggregated word counts:");
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

            // Simulate sending data to Reduce nodes (you can implement more logic here)
            for (int i = 0; i < numberOfReduces; i++) {
                System.out.println("Coordinator: Waiting for Reducer " + i);
                Socket reduceSocket = reduceServerSocket.accept();
                // Handle communication with Reduce nodes here
                System.out.println("Coordinator: Reducer " + i + " connected");

                // For now, just send aggregated word counts to the reducer (for simplicity)
                PrintWriter out = new PrintWriter(reduceSocket.getOutputStream(), true);
                for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                    out.println(entry.getKey() + ":" + entry.getValue());
                }

                reduceSocket.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

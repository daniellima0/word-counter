
// Coordinator.java
import java.io.*;
import java.net.*;
import java.util.*;

public class Coordinator {
    private static final int MAP_PORT = 3001;
    private static final int REDUCE_PORT = 5001;

    public static void main(String[] args) {
        int numberOfMaps = 3;
        int numberOfReduces = 1;

        // If arguments are provided, the values will be overwritten
        if (args.length == 2) {
            try {
                numberOfMaps = Integer.parseInt(args[0]);
                numberOfReduces = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid argument format. Using default values.");
            }
        } else {
            System.out.println("No arguments provided, using default values: " + numberOfMaps + " Map(s), " + numberOfReduces + " Reduce(s).");
        }

        List<Map<String, Integer>> mapperResults = new ArrayList<>();

        try (ServerSocket mapServerSocket = new ServerSocket(MAP_PORT);
             ServerSocket reduceServerSocket = new ServerSocket(REDUCE_PORT)) {

            // Accept and assign tasks to Map nodes
            for (int i = 0; i < numberOfMaps; i++) {
                System.out.println("Coordinator: Waiting for Mapper " + i);
                Socket mapSocket = mapServerSocket.accept();
                // Handle communication with Map nodes here
                System.out.println("Coordinator: Mapper " + i + " connected");

                 // Receber dados dos mappers e salvar em uma lista para enviar aos reducers
                 BufferedReader mapIn = new BufferedReader(new InputStreamReader(mapSocket.getInputStream()));
                 Map<String, Integer> wordCounts = new HashMap<>();
                 String line;
                 while ((line = mapIn.readLine()) != null) {
                     String[] parts = line.split(":");
                     if (parts.length == 2) {
                         String word = parts[0];
                         int count = Integer.parseInt(parts[1]);
                         wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
                     }
                 }
                 mapperResults.add(wordCounts);
                 mapSocket.close();
             }
             // Enviar os dados dos mappers para os reducers
            for (int i = 0; i < numberOfReduces; i++) {
                System.out.println("Coordinator: Waiting for Reducer " + i);
                Socket reduceSocket = reduceServerSocket.accept();
                // Handle communication with Reduce nodes here
                System.out.println("Coordinator: Reducer " + i + " connected");

                // Enviar os dados dos mappers para o reducer
                PrintWriter out = new PrintWriter(reduceSocket.getOutputStream(), true);
                // Distribuindo os dados de forma balanceada para os reducers
                for (Map<String, Integer> wordCount : mapperResults) {
                    for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
                        out.println(entry.getKey() + ":" + entry.getValue());
                    }
                }

                reduceSocket.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

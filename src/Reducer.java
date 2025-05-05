// Reducer.java
import java.io.*;
import java.net.*;
import java.util.*;

public class Reducer {
    //private static final int DEFAULT_COORDINATOR_PORT = 5001; // to coordinator
    private static final int DEFAULT_COORDINATOR_PORT = 9999; // to mock
    private static final String DEFAULT_COORDINATOR_ADDRESS = "localhost"; // Endereço padrão

    public static void main(String[] args) {
        
        String coordinatorAddress = DEFAULT_COORDINATOR_ADDRESS;
        int coordinatorPort = DEFAULT_COORDINATOR_PORT;
        int reducerId = 0;
        int numberOfReducers = 1;
    
         // Espera-se: Reducer <CoordinatorAddress> <CoordinatorPort> <ReducerId> <NumReducers>
         if (args.length == 4) {
            coordinatorAddress = args[0];
            coordinatorPort = Integer.parseInt(args[1]);
            reducerId = Integer.parseInt(args[2]);
            numberOfReducers = Integer.parseInt(args[3]);
        } else {
            System.out.println("Usage: Reducer <CoordinatorAddress> <CoordinatorPort> <ReducerId> <NumReducers>");
            return;
        }

        // Cria a conexão com o coordinator e lê os dados
        try (Socket socket = new Socket(coordinatorAddress, coordinatorPort);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
             ) {

            System.out.println("Reducer " + reducerId + " connected to Coordinator at " + coordinatorAddress + ":" + coordinatorPort);
            
            Map<String, Integer> aggregatedResults = new HashMap<>();
            String line;

            while ((line = in.readLine()) != null) {
                String[] parts = line.split(":");

                if (parts.length == 2) {
                    String word = parts[0];
                    int count;

                    try {
                        count = Integer.parseInt(parts[1]);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid count format for word: " + word);
                        continue;
                    }

                    // Cada reducer só processa palavras cujo hash % numReducers == reducerId
                    int targetReducer = Math.abs(word.hashCode()) % numberOfReducers;
                    if (targetReducer != reducerId) {
                        continue;
                    }

                    aggregatedResults.put(word, aggregatedResults.getOrDefault(word, 0) + count);
                }
            }

            System.out.println("\n--- Final Aggregated Word Count by Reducer " + reducerId + " ---");
            for (Map.Entry<String, Integer> entry : aggregatedResults.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

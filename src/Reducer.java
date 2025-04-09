// Reducer.java
import java.io.*;
import java.net.*;
import java.util.*;

public class Reducer {
    private static final int DEFAULT_COORDINATOR_PORT = 5001; // Porta padrão
    private static final String DEFAULT_COORDINATOR_ADDRESS = "localhost"; // Endereço padrão

    public static void main(String[] args) {
        //if (args.length != 2) {
         //   System.out.println("Usage: Reducer <CoordinatorAddress> <CoordinatorPort>");
          //  return;
        //}
        String coordinatorAddress = DEFAULT_COORDINATOR_ADDRESS;
        int coordinatorPort = DEFAULT_COORDINATOR_PORT;
        //String coordinatorAddress = args[0];
        //int coordinatorPort = Integer.parseInt(args[1]);
        // Se argumentos forem passados, use-os para configurar o endereço e a porta do Coordinator
        if (args.length == 2) {
            coordinatorAddress = args[0];
            coordinatorPort = Integer.parseInt(args[1]);
        } else {
            System.out.println("Usage: Reducer <CoordinatorAddress> <CoordinatorPort>");
        }

        //create the connection with the coordinator and read the data
        try (Socket socket = new Socket(coordinatorAddress, coordinatorPort);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
             ) {

            System.out.println("Reducer connected to Coordinator at " + coordinatorAddress + ":" + coordinatorPort);
            
            //map to store the result
            Map<String, Integer> aggregatedResults = new HashMap<>();
            String line;

            //to read every line 
            while ((line = in.readLine()) != null) {
                String[] parts = line.split(":");

                //make sure of the division between word and number of count
                if (parts.length == 2) {
                    String word = parts[0];
                    int count;

                    try {
                        count = Integer.parseInt(parts[1]); 
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid count format for word: " + word);
                        continue; 
                    }

                    //add the counts for the same word
                    aggregatedResults.put(word, aggregatedResults.getOrDefault(word, 0) + count);
                }
            }

            //output final result
            System.out.println("\n--- Final Aggregated Word Count ---");
            for (Map.Entry<String, Integer> entry : aggregatedResults.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

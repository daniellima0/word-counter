
// Coordinator.java
import java.io.*;
import java.net.*;
import java.util.*;

public class Coordinator {
    private static final int MAP_PORT = 3001;
    private static final int REDUCE_PORT = 5001;

    public static void main(String[] args) {

        //int numberOfMaps = Integer.parseInt(args[0]);
        //int numberOfReduces = Integer.parseInt(args[1]);
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

        try (//ServerSocket mapServerSocket = new ServerSocket(MAP_PORT);
                ServerSocket reduceServerSocket = new ServerSocket(REDUCE_PORT)) {

            // Accept and assign tasks to Map nodes
            /* 
            for (int i = 0; i < numberOfMaps; i++) {
                System.out.println("Coordinator: Waiting for Mapper " + i);
                Socket mapSocket = mapServerSocket.accept();
                // Handle communication with Map nodes here
                System.out.println("Coordinator: Mapper " + i + " connected");
            }
                */

            // Simulate sending data to Reduce nodes (you can implement a more sophisticated
            // logic here)
            for (int i = 0; i < numberOfReduces; i++) {
                System.out.println("Coordinator: Waiting for Reducer " + i);
                Socket reduceSocket = reduceServerSocket.accept();
                // Handle communication with Reduce nodes here
                System.out.println("Coordinator: Reducer " + i + " connected");

                /* 
                // Simulated word count data to send to the Reducer
                PrintWriter out = new PrintWriter(reduceSocket.getOutputStream(), true);
                out.println("gato:5");
                out.println("cachorro:3");
                out.println("passaro:1");
                out.println("gato:2");
                out.println("gato:3");
                out.println("cachorro:2");
                out.println("passaro:2");
                out.println("gato:1");

                // Close the socket after sending data
                reduceSocket.close();
                */
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

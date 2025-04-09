
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

        try (ServerSocket mapServerSocket = new ServerSocket(MAP_PORT);
                ServerSocket reduceServerSocket = new ServerSocket(REDUCE_PORT)) {

            // Accept and assign tasks to Map nodes
            for (int i = 0; i < numberOfMaps; i++) {
                System.out.println("Coordinator: Waiting for Mapper " + i);
                Socket mapSocket = mapServerSocket.accept();
                // Handle communication with Map nodes here
                System.out.println("Coordinator: Mapper " + i + " connected");
            }

            // Simulate sending data to Reduce nodes (you can implement a more sophisticated
            // logic here)
            for (int i = 0; i < numberOfReduces; i++) {
                System.out.println("Coordinator: Waiting for Reducer " + i);
                Socket reduceSocket = reduceServerSocket.accept();
                // Handle communication with Reduce nodes here
                System.out.println("Coordinator: Reducer " + i + " connected");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

import java.io.*;
import java.net.*;
import java.util.*;

public class Reducer {
    public static void main(String[] args) {
        String coordinatorAddress = "localhost";
        int reducePort = 5001;
        int resultPort = 6001;

        try (
                Socket socket = new Socket(coordinatorAddress, reducePort);
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            int reducerId = Integer.parseInt(in.readLine());
            int totalReducers = Integer.parseInt(in.readLine());

            System.out.println("Reducer " + reducerId + " started.");

            Map<String, Integer> wordCounts = new HashMap<>();
            String line;
            while ((line = in.readLine()) != null) {
                if (line.equals("<<END>>"))
                    break;

                String[] parts = line.split(":");
                if (parts.length != 2)
                    continue;

                String word = parts[0];
                int count = Integer.parseInt(parts[1]);

                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
            }

            // Print reducer's local result
            System.out.println("Reducer " + reducerId + " local result:");
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

            // Send final result back to coordinator
            Socket returnSocket = new Socket(coordinatorAddress, resultPort);
            PrintWriter out = new PrintWriter(returnSocket.getOutputStream(), true);
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                out.println(entry.getKey() + ":" + entry.getValue());
            }
            out.println("<<END>>");
            out.close();
            returnSocket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

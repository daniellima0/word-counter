import java.io.*;
import java.net.*;
import java.util.*;

public class Reducer {
    public static void main(String[] args) {
        String coordinatorAddress = "localhost";
        int port = 5001;

        try (
                Socket socket = new Socket(coordinatorAddress, port);
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

            System.out.println("Reducer " + reducerId + " final counts:");
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

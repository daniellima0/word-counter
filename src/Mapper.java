import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;

public class Mapper {
    public static void main(String[] args) {
        String coordinatorAddress = "localhost";
        Integer coordinatorPort = 3001;

        try (Socket socket = new Socket(coordinatorAddress, coordinatorPort);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String line;
            Map<String, Integer> wordCounts = new HashMap<>();
            while ((line = reader.readLine()) != null) {
                if (line.equals("<<END>>"))
                    break;

                String[] words = line.split("\\W+");
                for (String word : words) {
                    if (word.length() > 0) {
                        word = word.toLowerCase();
                        wordCounts.putIfAbsent(word, 0);
                        wordCounts.put(word, wordCounts.get(word) + 1);
                    }
                }
            }

            // Send word counts to Coordinator
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                out.println(entry.getKey() + ":" + entry.getValue());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

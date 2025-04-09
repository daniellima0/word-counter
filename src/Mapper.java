
// Mapper.java
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;

public class Mapper {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: Mapper <file>");
            return;
        }

        String fileName = args[0];

        String coordinatorAddress = "localhost";
        Integer coordinatorPort = 3000;

        try (Socket socket = new Socket(coordinatorAddress, coordinatorPort);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new FileReader(fileName))) {

            String line;
            Map<String, WordCount> wordCounts = new HashMap<>();
            while ((line = reader.readLine()) != null) {
                String[] words = line.split("\\W+");
                for (String word : words) {
                    if (word.length() > 0) {
                        word = word.toLowerCase();
                        wordCounts.putIfAbsent(word, new WordCount(word, 0));
                        wordCounts.get(word).incrementCount();
                    }
                }
            }

            // Send the word count results to Coordinator
            for (WordCount wc : wordCounts.values()) {
                out.println(wc.getWord() + ":" + wc.getCount());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

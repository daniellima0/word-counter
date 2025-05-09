import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Coordinator {
    private static final String FILE_PATH = "input/input1.txt";

    private static final int MAP_PORT = 3001;
    private static final int REDUCE_PORT = 5001;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: Coordinator <numberOfMaps> <numberOfReduces>");
            return;
        }

        int numberOfMaps = Integer.parseInt(args[0]);
        int numberOfReduces = Integer.parseInt(args[1]);

        // Read entire file and tokenize by words
        List<String> allWords = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(FILE_PATH))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] words = line.split("\\W+");
                for (String word : words) {
                    if (!word.isEmpty()) {
                        allWords.add(word);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // Split words evenly among mappers
        int totalWords = allWords.size();
        int chunkSize = (int) Math.ceil((double) totalWords / numberOfMaps);
        List<List<String>> mapperChunks = new ArrayList<>();

        for (int i = 0; i < numberOfMaps; i++) {
            int startIdx = i * chunkSize;
            int endIdx = Math.min(startIdx + chunkSize, totalWords);
            mapperChunks.add(new ArrayList<>(allWords.subList(startIdx, endIdx)));
        }

        // Partitioned data per reducer
        Map<Integer, List<String>> partitionedData = new ConcurrentHashMap<>();
        for (int i = 0; i < numberOfReduces; i++) {
            partitionedData.put(i, Collections.synchronizedList(new ArrayList<>()));
        }

        ExecutorService executor = Executors.newFixedThreadPool(numberOfMaps);

        try (
                ServerSocket mapServerSocket = new ServerSocket(MAP_PORT);
                ServerSocket reduceServerSocket = new ServerSocket(REDUCE_PORT)) {
            System.out.println("Coordinator: Waiting for Mappers to connect...");

            for (int i = 0; i < numberOfMaps; i++) {
                Socket mapperSocket = mapServerSocket.accept();
                executor.execute(
                        new MapperHandler(mapperSocket, mapperChunks.get(i), partitionedData, numberOfReduces, i));
            }

            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MINUTES);

            // Send data to reducers
            for (int i = 0; i < numberOfReduces; i++) {
                System.out.println("Coordinator: Waiting for Reducer " + i);
                Socket reduceSocket = reduceServerSocket.accept();
                PrintWriter out = new PrintWriter(reduceSocket.getOutputStream(), true);

                out.println(i); // reducer ID
                out.println(numberOfReduces);

                for (String pair : partitionedData.get(i)) {
                    out.println(pair);
                }

                out.println("<<END>>");
                reduceSocket.close();
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class MapperHandler implements Runnable {
        private Socket socket;
        private List<String> wordChunk;
        private Map<Integer, List<String>> partitionedData;
        private int numberOfReduces;
        private int mapperId;

        public MapperHandler(Socket socket, List<String> wordChunk,
                Map<Integer, List<String>> partitionedData, int numberOfReduces, int mapperId) {
            this.socket = socket;
            this.wordChunk = wordChunk;
            this.partitionedData = partitionedData;
            this.numberOfReduces = numberOfReduces;
            this.mapperId = mapperId;
        }

        @Override
        public void run() {
            try (
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                System.out.println("Coordinator: Sending chunk to Mapper " + mapperId);

                for (String word : wordChunk) {
                    out.println(word);
                }
                out.println("<<END>>");

                // Collect raw word:1 pairs and partition
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.trim().isEmpty())
                        continue;
                    String[] parts = line.split(":");
                    if (parts.length != 2)
                        continue;

                    String word = parts[0];
                    int reducerId = customHash(word, numberOfReduces);
                    partitionedData.get(reducerId).add(line);

                    // Log mapper output
                    System.out.println("Mapper " + mapperId + " emitted: " + line + " (to reducer " + reducerId + ")");
                }

                socket.close();
                System.out.println("Coordinator: Mapper " + mapperId + " completed");

            } catch (IOException e) {
                System.err.println("MapperHandler (Mapper " + mapperId + ") error: " + e.getMessage());
            }
        }
    
    }

    public static int customHash(String word, int numberOfReduces) {
        int hash = 0;
        for (int i = 0; i < word.length(); i++) {
            hash = 31 * hash + word.charAt(i);
        }
        return Math.abs(hash % numberOfReduces);
    }
}

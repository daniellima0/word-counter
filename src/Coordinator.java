import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Coordinator {
    private static final int MAP_PORT = 3001;
    private static final int REDUCE_PORT = 5001;

    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Coordinator <filepath> <numberOfMaps> <numberOfReduces>");
            return;
        }

        String fileName = args[0];
        int numberOfMaps = Integer.parseInt(args[1]);
        int numberOfReduces = Integer.parseInt(args[2]);

        File file = new File(fileName);
        long fileSize = file.length();
        long chunkSize = fileSize / numberOfMaps;

        ConcurrentHashMap<String, Integer> wordCounts = new ConcurrentHashMap<>();

        ExecutorService executor = Executors.newFixedThreadPool(numberOfMaps);

        try (
                ServerSocket mapServerSocket = new ServerSocket(MAP_PORT);
                ServerSocket reduceServerSocket = new ServerSocket(REDUCE_PORT);) {
            System.out.println("Coordinator: Waiting for Mappers to connect...");

            // Asynchronously send data to all mappers
            for (int i = 0; i < numberOfMaps; i++) {
                Socket mapperSocket = mapServerSocket.accept();
                long startByte = i * chunkSize;
                long endByte = (i == numberOfMaps - 1) ? fileSize : (i + 1) * chunkSize;

                // Execute each mapper handler asynchronously
                executor.execute(new MapperHandler(mapperSocket, fileName, startByte, endByte, wordCounts, i));
            }

            executor.shutdown(); // No more tasks will be submitted to the executor
            executor.awaitTermination(5, TimeUnit.MINUTES); // Wait for all mapper tasks to finish

            // Display aggregated results
            System.out.println("Coordinator: Aggregated word counts:");
            for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

            // Accept Reducer connections and send them data
            for (int i = 0; i < numberOfReduces; i++) {
                System.out.println("Coordinator: Waiting for Reducer " + i);
                Socket reduceSocket = reduceServerSocket.accept();
                System.out.println("Coordinator: Reducer " + i + " connected");

                PrintWriter out = new PrintWriter(reduceSocket.getOutputStream(), true);

                // Send reducer ID and total reducers
                out.println(i); // reducerId
                out.println(numberOfReduces);

                // Send all word counts
                for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                    out.println(entry.getKey() + ":" + entry.getValue());
                }

                reduceSocket.close();
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class MapperHandler implements Runnable {
        private Socket socket;
        private String fileName;
        private long startByte, endByte;
        private ConcurrentHashMap<String, Integer> wordCounts;
        private int mapperId;

        public MapperHandler(Socket socket, String fileName, long startByte, long endByte,
                ConcurrentHashMap<String, Integer> wordCounts, int mapperId) {
            this.socket = socket;
            this.fileName = fileName;
            this.startByte = startByte;
            this.endByte = endByte;
            this.wordCounts = wordCounts;
            this.mapperId = mapperId;
        }

        @Override
        public void run() {
            try (
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {

                System.out.println("Coordinator: Sending chunk to Mapper " + mapperId);

                // Position the file pointer
                raf.seek(startByte);

                long bytesSent = 0;
                while (bytesSent < (endByte - startByte)) {
                    String line = raf.readLine();
                    if (line == null)
                        break;
                    out.println(line);
                    bytesSent += line.getBytes().length;
                }
                out.println("<<END>>"); // signal end of data

                // Receive results from Mapper
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.trim().isEmpty())
                        continue;
                    String[] parts = line.split(":");
                    if (parts.length != 2)
                        continue;

                    String word = parts[0];
                    int count = Integer.parseInt(parts[1]);

                    wordCounts.merge(word, count, Integer::sum);
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
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Coordinator {
    private static final String INPUT_FOLDER = "../input/";

    private static final int MAP_PORT = 3001;
    private static final int REDUCE_PORT = 5001;
    private static final int RESULT_PORT = 6001;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: Coordinator <numberOfReduces>");
            return;
        }

        int numberOfReduces = Integer.parseInt(args[0]);

        // Read all .txt files from the input folder
        File folder = new File(INPUT_FOLDER);
        File[] inputFiles = folder.listFiles((dir, name) -> name.endsWith(".txt"));

        if (inputFiles == null || inputFiles.length == 0) {
            System.out.println("No input files found in " + INPUT_FOLDER);
            return;
        }

        int numberOfMaps = inputFiles.length;
        System.out.println("Found " + numberOfMaps + " input files. Spawning " + numberOfMaps + " mappers.");

        List<List<String>> mapperChunks = new ArrayList<>();

        for (File file : inputFiles) {
            List<String> words = new ArrayList<>();
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] tokens = line.split("\\W+");
                    for (String word : tokens) {
                        if (!word.isEmpty()) {
                            words.add(word.toLowerCase());
                        }
                    }
                }
            } catch (IOException e) {
                System.err.println("Error reading file: " + file.getName());
                e.printStackTrace();
                return;
            }
            mapperChunks.add(words);
        }

        // Partitioned data per reducer
        Map<Integer, List<String>> partitionedData = new ConcurrentHashMap<>();
        for (int i = 0; i < numberOfReduces; i++) {
            partitionedData.put(i, Collections.synchronizedList(new ArrayList<>()));
        }

        ExecutorService executor = Executors.newFixedThreadPool(numberOfMaps);

        try (
                ServerSocket mapServerSocket = new ServerSocket(MAP_PORT);
                ServerSocket reduceServerSocket = new ServerSocket(REDUCE_PORT);
                ServerSocket resultSocket = new ServerSocket(RESULT_PORT)) {
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

            // Collect final results from reducers
            Map<String, Integer> finalResults = new HashMap<>();
            for (int i = 0; i < numberOfReduces; i++) {
                Socket returnSocket = resultSocket.accept();
                BufferedReader in = new BufferedReader(new InputStreamReader(returnSocket.getInputStream()));

                String line;
                while ((line = in.readLine()) != null) {
                    if (line.equals("<<END>>"))
                        break;
                    String[] parts = line.split(":");
                    if (parts.length != 2)
                        continue;
                    String word = parts[0];
                    int count = Integer.parseInt(parts[1]);

                    finalResults.put(word, finalResults.getOrDefault(word, 0) + count);
                }

                returnSocket.close();
            }

            // Print final consolidated result
            System.out.println("\nFinal Consolidated Word Count:");
            for (Map.Entry<String, Integer> entry : finalResults.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

            // Write the final results to a file (final_count.txt)
            File outputDir = new File("../output");
            if (!outputDir.exists()) {
                outputDir.mkdir(); // Create the output folder if it doesn't exist
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(new File(outputDir, "final_count.txt")))) {
                for (Map.Entry<String, Integer> entry : finalResults.entrySet()) {
                    writer.write(entry.getKey() + ": " + entry.getValue());
                    writer.newLine();
                }
                System.out.println("Final word count saved to 'output/final_count.txt'.");
            } catch (IOException e) {
                System.err.println("Error writing to final_count.txt: " + e.getMessage());
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

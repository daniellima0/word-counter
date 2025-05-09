import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Coordinator {
    private static final String FILE_PATH = "input.txt";

    private static final int MAP_PORT = 3001;
    private static final int REDUCE_PORT = 5001;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: Coordinator <numberOfMaps> <numberOfReduces>");
            return;
        }

        int numberOfMaps = Integer.parseInt(args[0]);
        int numberOfReduces = Integer.parseInt(args[1]);

        File file = new File(FILE_PATH);
        long fileSize = file.length();
        long chunkSize = fileSize / numberOfMaps;

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
                long startByte = i * chunkSize;
                long endByte = (i == numberOfMaps - 1) ? fileSize : (i + 1) * chunkSize;

                executor.execute(new MapperHandler(mapperSocket, FILE_PATH, startByte, endByte, partitionedData,
                        numberOfReduces, i));
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
        private String fileName;
        private long startByte, endByte;
        private Map<Integer, List<String>> partitionedData;
        private int numberOfReduces;
        private int mapperId;

        public MapperHandler(Socket socket, String fileName, long startByte, long endByte,
                Map<Integer, List<String>> partitionedData, int numberOfReduces, int mapperId) {
            this.socket = socket;
            this.fileName = fileName;
            this.startByte = startByte;
            this.endByte = endByte;
            this.partitionedData = partitionedData;
            this.numberOfReduces = numberOfReduces;
            this.mapperId = mapperId;
        }

        @Override
        public void run() {
            try (
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
                System.out.println("Coordinator: Sending chunk to Mapper " + mapperId);

                raf.seek(startByte);
                long bytesSent = 0;
                while (bytesSent < (endByte - startByte)) {
                    String line = raf.readLine();
                    if (line == null)
                        break;
                    out.println(line);
                    bytesSent += line.getBytes().length;
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

                    // 👇 Log mapper output
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

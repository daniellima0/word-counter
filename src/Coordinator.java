import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Coordinator {
    private static final String INPUT_FOLDER = "input/";

    private static final int MAP_PORT = 3001;
    private static final int REDUCE_PORT = 5001;
    private static final int RESULT_PORT = 6001;

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: Coordinator <numberOfReduces>");
            return;
        }

        int numberOfReduces = Integer.parseInt(args[0]);

        // Ler todos os arquivos .txt da pasta de entrada
        File folder = new File(INPUT_FOLDER);
        File[] inputFiles = folder.listFiles((dir, name) -> name.endsWith(".txt"));

        if (inputFiles == null || inputFiles.length == 0) {
            System.out.println("No input files found in " + INPUT_FOLDER);
            return;
        }

        int numberOfMaps = inputFiles.length;
        System.out.println("Found " + numberOfMaps + " input files. Spawning " + numberOfMaps + " mappers.");

        List<List<String>> mapperChunks = new ArrayList<>();

        // Prepara os chunks de palavras para os mappers
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

        // Particiona os dados para os reducers
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

            // Aceita as conexões dos Mappers e distribui os chunks
            for (int i = 0; i < numberOfMaps; i++) {
                Socket mapperSocket = mapServerSocket.accept();
                executor.execute(
                        new MapperHandler(mapperSocket, mapperChunks.get(i), partitionedData, numberOfReduces, i));
            }

            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.MINUTES);

            // Envia dados para os reducers
            for (int i = 0; i < numberOfReduces; i++) {
                System.out.println("Coordinator: Waiting for Reducer " + i);
                Socket reduceSocket = reduceServerSocket.accept();
                PrintWriter out = new PrintWriter(reduceSocket.getOutputStream(), true);

                out.println(i); // ID do reducer
                out.println(numberOfReduces);

                for (String pair : partitionedData.get(i)) {
                    out.println(pair);
                }

                out.println("<<END>>");
                reduceSocket.close();
            }

            // Coleta resultados finais dos reducers
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

            // Exibe e grava o resultado final
            System.out.println("\nFinal Consolidated Word Count:");
            for (Map.Entry<String, Integer> entry : finalResults.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }

            // Grava os resultados em um arquivo
            File outputDir = new File("output");
            if (!outputDir.exists()) {
                outputDir.mkdir();
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
}

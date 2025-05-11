import java.io.*;
import java.net.*;
import java.util.*;

public class MapperHandler implements Runnable {
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

            // Envia os chunks de palavras para o worker Map
            for (String word : wordChunk) {
                out.println(word);
            }
            out.println("<<END>>");

            // Coleta os pares (word:1) de cada Mapper e faz a partição
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

                System.out.println("Mapper " + mapperId + " emitted: " + line + " (to reducer " + reducerId + ")");
            }

            socket.close();
            System.out.println("Coordinator: Mapper " + mapperId + " completed");

        } catch (IOException e) {
            System.err.println("MapperHandler (Mapper " + mapperId + ") error: " + e.getMessage());
        }
    }

    // Função de hash customizada para distribuir as palavras entre os reducers
    public static int customHash(String word, int numberOfReduces) {
        int hash = 0;
        for (int i = 0; i < word.length(); i++) {
            hash = 31 * hash + word.charAt(i);
        }
        return Math.abs(hash % numberOfReduces);
    }
}

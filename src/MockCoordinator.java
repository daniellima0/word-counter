import java.io.*;
import java.net.*;

public class MockCoordinator {

    public static int customHash(String word, int numberOfReduces) {
        int hash = 0;
        for (int i = 0; i < word.length(); i++) {
            hash = 31 * hash + word.charAt(i);  // multiplicação por 31 é uma prática comum para o cálculo de hash
        }
        // Usar modulo para garantir que o valor fique dentro do número de reducers
        return Math.abs(hash % numberOfReduces);
    }


    public static void main(String[] args) {
        int port = 9999; 
        int numberOfReduces = 2; 

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Mock Coordinator waiting for connection...");

            try (Socket socket = serverSocket.accept();
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                System.out.println("Reducer connected!");

                // Dados simulados no formato "palavra:contagem"
                String[] words = {
                    "gato:5", "cachorro:3", "gato:2", "zebra:1","passaro:1", "gato:5", "cachorro:3", "gato:2", "passaro:1", "gato:5", "cachorro:3", 
                    "gato:2", "passaro:1", "gato:5", "cachorro:3", "gato:2", "passaro:1", "zebra:1", "gorila:2", "hipopotamo:5",
                    "zebra:4", "hipopotamo:2", "gorila:5"
                
                };

                // Enviar dados simulados e distribuí-los para os reducers
                for (String wordCountStr : words) {
                    String[] parts = wordCountStr.split(":");
                    String word = parts[0];
                    int count = Integer.parseInt(parts[1]);

                    // Verificando hashCode e o índice
                    int reducerIndex = customHash(word, numberOfReduces);

                    System.out.println("HashCode da palavra " + word + ": " + word.hashCode());
                    System.out.println("Índice do Reducer: " + reducerIndex);

                    System.out.println("Distribuindo palavra: " + word + " para o Reducer: " + reducerIndex);

                    // Enviar a palavra para o reducer específico
                    out.println(word + ":" + count);
                }

                // Fechar conexão depois de enviar os dados
                out.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

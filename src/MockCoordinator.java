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
        int numberOfReduces = 2;  // Número de reducers a ser distribuído

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Mock Coordinator waiting for connection...");

            // Aceitar múltiplas conexões de reducers
            try (Socket socket1 = serverSocket.accept();
                 Socket socket2 = serverSocket.accept()) {

                PrintWriter out1 = new PrintWriter(socket1.getOutputStream(), true);
                PrintWriter out2 = new PrintWriter(socket2.getOutputStream(), true);

                System.out.println("Reducers connected!");

                // Dados simulados no formato "palavra:contagem"
                String[] words = {
                    "gato:5", "cachorro:3", "gato:2", "zebra:1","passaro:1", "gato:5", 
                    "cachorro:3", "gato:2", "passaro:1", "gato:5", "cachorro:3", 
                    "gato:2", "passaro:1", "gato:5", "cachorro:3", "gato:2", 
                    "passaro:1", "zebra:1", "gorila:2", "hipopotamo:5", "zebra:4", 
                    "hipopotamo:2", "gorila:5"
                };

                // Enviar dados simulados e distribuí-los para os reducers
                for (String wordCountStr : words) {
                    String[] parts = wordCountStr.split(":");
                    String word = parts[0];
                    int count = Integer.parseInt(parts[1]);

                    // Verificando hashCode e o índice do reducer
                    int reducerIndex = customHash(word, numberOfReduces);

                    System.out.println("HashCode da palavra " + word + ": " + word.hashCode());
                    System.out.println("Índice do Reducer: " + reducerIndex);

                    System.out.println("Distribuindo palavra: " + word + " para o Reducer: " + reducerIndex);

                    // Distribuir a palavra para o reducer correto
                    if (reducerIndex == 0) {
                        out1.println(word + ":" + count);
                    } else if (reducerIndex == 1) {
                        out2.println(word + ":" + count);
                    }
                }

                // Fechar conexões depois de enviar os dados
                out1.close();
                out2.close();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

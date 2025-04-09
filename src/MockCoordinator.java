// MockCoordinator.java - Servidor mock para testar o Reducer
import java.io.*;
import java.net.*;

public class MockCoordinator {
    public static void main(String[] args) {
        int port = 9999; // Porta onde o coordenador mock vai escutar

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Mock Coordinator waiting for connection...");

            try (Socket socket = serverSocket.accept();
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                System.out.println("Reducer connected!");

                // Enviar dados simulados no formato "palavra:contagem"
                out.println("gato:5");
                out.println("cachorro:3");
                out.println("gato:2");
                out.println("passaro:1");

                // Fechar conexão depois de enviar os dados
                out.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

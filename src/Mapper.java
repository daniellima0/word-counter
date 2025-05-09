import java.io.*;
import java.net.*;

public class Mapper {
    public static void main(String[] args) {
        String coordinatorAddress = "localhost";
        int coordinatorPort = 3001;

        try (
                Socket socket = new Socket(coordinatorAddress, coordinatorPort);
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.equals("<<END>>"))
                    break;

                String[] words = line.split("\\W+");
                for (String word : words) {
                    if (!word.isEmpty()) {
                        out.println(word.toLowerCase() + ":1"); // Emit (word, 1) without aggregation
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

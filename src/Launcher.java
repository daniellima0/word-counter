import java.io.IOException;

public class Launcher {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java Launcher <numMappers> <numReducers>");
            return;
        }

        int numMappers = Integer.parseInt(args[0]);
        int numReducers = Integer.parseInt(args[1]);

        try {
            // 1. Start the Coordinator
            new ProcessBuilder("java", "-cp", "bin", "Coordinator", String.valueOf(numMappers),
                    String.valueOf(numReducers))
                    .inheritIO()
                    .start();

            Thread.sleep(1000); // Wait for Coordinator to open sockets

            // 2. Start the Mappers
            for (int i = 0; i < numMappers; i++) {
                new ProcessBuilder("java", "-cp", "bin", "Mapper")
                        .inheritIO()
                        .start();
            }

            Thread.sleep(2000); // Wait for Mappers to finish

            // 3. Start the Reducers
            for (int i = 0; i < numReducers; i++) {
                new ProcessBuilder("java", "-cp", "bin", "Reducer")
                        .inheritIO()
                        .start();
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

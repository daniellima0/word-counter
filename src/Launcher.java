import java.io.IOException;

public class Launcher {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Uso: java Launcher <numMappers> <numReducers>");
            return;
        }

        int numMappers = Integer.parseInt(args[0]);
        int numReducers = Integer.parseInt(args[1]);

        try {
            // 1. Iniciar o Coordinator
            new ProcessBuilder("java", "-cp", "bin", "Coordinator", String.valueOf(numMappers),
                    String.valueOf(numReducers))
                    .inheritIO()
                    .start();

            Thread.sleep(1000); // Espera para os sockets abrirem

            // 2. Iniciar os Mappers
            for (int i = 0; i < numMappers; i++) {
                new ProcessBuilder("java", "-cp", "bin", "Mapper")
                        .inheritIO()
                        .start();
            }

            Thread.sleep(2000); // Espera os Mappers completarem

            // 3. Iniciar os Reducers
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

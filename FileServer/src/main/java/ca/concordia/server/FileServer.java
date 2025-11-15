package ca.concordia.server;

import ca.concordia.filesystem.FileSystemManager;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class FileServer {

    private FileSystemManager fsManager;
    private int port;

    public FileServer(int port, String fileSystemName, int totalSize){
        this.fsManager = new FileSystemManager(fileSystemName, totalSize);
        this.port = port;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server started. Listening on port " + port + "...");

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Accepted connection: " + clientSocket);

                //  MULTITHREADING: Handle each client 
                new Thread(() -> handleClient(clientSocket)).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Could not start server on port " + port);
        }
    }

    //Client thread
    private void handleClient(Socket clientSocket) {
        System.out.println("Handling client in thread: " + Thread.currentThread().getName());

        try (
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter writer = new PrintWriter(
                    clientSocket.getOutputStream(), true)
        ) {
            String line;

            while ((line = reader.readLine()) != null) {

                System.out.println("Received from client: " + line);
                String[] parts = line.split(" ", 3);
                String command = parts[0].toUpperCase();

                try {
                    //commands
                    switch (command) {
                        
                        case "CREATE":
                            if (parts.length < 2) {
                                writer.println("ERROR: Missing filename.");
                                break;
                            }
                            fsManager.createFile(parts[1]);
                            writer.println("SUCCESS: File '" + parts[1] + "' created.");
                            break;

                        case "WRITE":
                            if (parts.length < 3) {
                                writer.println("ERROR: Usage: WRITE <filename> <data>");
                                break;
                            }
                            fsManager.writeFile(parts[1], parts[2].getBytes());
                            writer.println("SUCCESS: '" + parts[2] + "' written to " + parts[1]);
                            break;

                        case "READ":
                            if (parts.length < 2) {
                                writer.println("ERROR: Missing filename.");
                                break;
                            }
                            byte[] result = fsManager.readFile(parts[1]);
                            writer.println(new String(result));
                            break;

                        case "DELETE":
                            if (parts.length < 2) {
                                writer.println("ERROR: Missing filename.");
                                break;
                            }
                            fsManager.deleteFile(parts[1]);
                            writer.println("SUCCESS: File '" + parts[1] + "' deleted.");
                            break;

                        case "LIST":
                            writer.println(fsManager.listFiles());
                            break;

                        case "QUIT":
                            writer.println("SUCCESS: Disconnecting.");
                            return;

                        default:
                            writer.println("ERROR: Unknown command.");
                            break;
                    }

                } catch (Exception e) {
                    writer.println("ERROR: " + e.getMessage());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { clientSocket.close(); } catch (Exception ignored) {}
            System.out.println("Client disconnected.");
        }
    }
}

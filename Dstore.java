import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLOutput;

public class Dstore {

    private static final String BLACK = "\u001B[30m";
    private static final String RED = "\u001B[31m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String BLUE = "\u001B[34m";
    private static final String PURPLE = "\u001B[35m";
    private static final String CYAN = "\u001B[36m";
    private static final String WHITE = "\u001B[37m";

    //Dstore listening port
    private static int port;
    //Controller's unique listening port
    private static int cport;
    //Failure detection timeout (in milliseconds)
    private static int timeout;
    //Unique file_folder
    private static String file_folder;
    //Keep the dedicated socket with the controller
    protected static Socket controller;

    /**
     * The main function is called as soon as the Controller is started
     * @param args are the command line arguments passed in the Controller (Explanations bellow)
     */
    public static void main(String[] args){
        port = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        file_folder = args[3];//Path.of(args[3]);

        /**
         * Establishing connection with the controller
         */
        try {
            Socket helloSocket = new Socket("127.0.0.1", cport);
            //The hello socket is socket of the controller
            controller = helloSocket;

            PrintWriter controllerOut = new PrintWriter(helloSocket.getOutputStream());

            controllerOut.println("DSTORE " + port);
            controllerOut.flush();
            System.out.println("[INFO]:Sending CONNECT " + port);
            ServerSocket ss = new ServerSocket(port);
            while(true){
                try{
                    Socket client = ss.accept();
                    new Thread(new DStoreServiceThread(client)).start();
                }
                catch (Exception e){
                    System.out.println("[ERROR]:Issue while establishing connection with client");
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class DStoreServiceThread implements Runnable {

        Socket client;

        DStoreServiceThread(Socket c) {
            client = c;
            System.out.println("[INFO]:Client " + client.getInetAddress() + " connected");
        }

        @Override
        public void run() {
            /**
             * Reading TEXTUAL MESSAGES
             */
            try {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(client.getInputStream()));
                String line = in.readLine();
                String[] args = line.split(" ");
                String command = args[0];
                System.out.println("Received:" + command);
                PrintWriter out = new PrintWriter(client.getOutputStream());

                if (command.equals("STORE")) {
                    String filename = args[1];
                    String filesize = args[2];
                    //Acknowledging readiness to receive file
                    out.println("ACK");
                    //Creating a new file with the given filename
                    File inputFile = new File(filename);
                    //Creating input one-way input stream with the connected client
                    FileInputStream fileIn = new FileInputStream(inputFile);
                    fileIn.readNBytes(Integer.parseInt(filesize));
                    fileIn.close();
                    PrintWriter informController = new PrintWriter(controller.getOutputStream());
                    informController.println("STORE_ACK "+filename);
                }
                else if (command.equals("LOAD_DATA")) {
                    String filename = args[1];
                    File folder = new File(file_folder);
                    File[] listOfFiles = folder.listFiles();

                    //Write file contents to a file called $filename
                    FileOutputStream outf = new FileOutputStream(filename);
                    outf.write(Files.readAllBytes(Path.of(filename)));

                    //If file doesn't exist => close the socket with the client
                    client.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

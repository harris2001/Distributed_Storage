import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;

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

    //Used for logging
//    private static FileWriter fw;

    /**
     * The main function is called as soon as the Controller is started
     * @param args are the command line arguments passed in the Controller (Explanations bellow)
     */
    public static void main(String[] args) throws IOException {
        port = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        file_folder = args[3];

        File dir = new File(file_folder);

        //If directory doesn't exist, mkdir
        if(!dir.exists()){
            dir.mkdir();
        }

        /**
         * Defining logger
         */
//        File logger = new File("logger.txt");
//        if (! logger.exists() && !logger.createNewFile())
//            throw new RuntimeException("Can't create logger.txt");

//        fw = new FileWriter(logger);
//
//        BufferedWriter bw = new BufferedWriter(fw);
//        bw.write("OK");
//        bw.close();

        /**
         * Establishing connection with the controller
         */
        controller = new Socket("127.0.0.1", cport);
        PrintWriter controllerOut = new PrintWriter(controller.getOutputStream());

        controllerOut.println("DSTORE " + port);
        controllerOut.flush();
        System.out.println("[INFO]:Sending CONNECT " + port);
        try{
            ServerSocket ss = new ServerSocket(port);
            System.out.println("Opened server socket on port "+port);
            for(;;){
                try{Socket client = ss.accept();
                    new Thread(new DStoreServiceThread(client)).start();
                }
                catch(Exception e){
                    System.out.println("error::::::::::::::::::: "+e);
                }
            }
        }
        catch (Exception e){
            System.out.println("[ERROR]:Issue while establishing connection with client");
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
                PrintWriter out = new PrintWriter(client.getOutputStream());
                String line = in.readLine();

                String[] args = line.split(" ");
                String command = args[0];
                System.out.println("Received:" + line);
                if (command.equals("ACK_DSTORE")) {
//                    controller = client;
                    //Sending hello server message
//                    PrintWriter controllerOut = new PrintWriter(controller.getOutputStream());
//                    controllerOut.println("hello server");
//                    controllerOut.flush();
                    System.out.println("[INFO]:Established connection with controller");
                }
                else if (command.equals("STORE")) {
                    String filename = args[1];
                    Integer filesize = Integer.parseInt(args[2]);

                    //Acknowledging readiness to receive file
                    out.println("ACK");
                    out.flush();

                    //Creating a new file with the given filename
                    File outputFile = new File(file_folder,filename);

                    //Create new file even if it exist
                    outputFile.createNewFile();

                    //Receiving data
                    FileOutputStream fout = new FileOutputStream(outputFile);
                    var file = client.getInputStream().readNBytes(filesize);

                    //Writing data to file
                    fout.write(file);

                    //Sending store acknowledgement to controller
                    PrintWriter informController = new PrintWriter(controller.getOutputStream());
                    send(informController,"STORE_ACK "+filename);

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
            } catch (FileNotFoundException ex) {
                ex.printStackTrace();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        private void send(PrintWriter out, String message) {
            System.out.println("SENDING: "+message);
            out.println(message);
            out.flush();
//            try {
//                BufferedWriter bw = new BufferedWriter(fw);
//                bw.write("SENDING: "+message);
//                bw.newLine();
//                bw.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }
    }
}
//TODO: fix issue:
//STORE_ACK is not send to the controller and file not saved locally by dstore

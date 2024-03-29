import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

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
        else{
            File[] listOfFiles = dir.listFiles();

            //Deleting the file from the dstore directory
            for(File file : listOfFiles){
               try {
                    Files.delete(Path.of(file.getAbsolutePath()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
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
        BufferedReader controllerIn = new BufferedReader(new InputStreamReader(controller.getInputStream()));

        controllerOut.println("JOIN " + port);
        controllerOut.flush();
        System.out.println("[INFO]:Sending CONNECT " + port);
        try{
            ServerSocket ss = new ServerSocket(port);
            System.out.println("Opened server socket on port "+port);
            //Creating controller listening thread
            new Thread(){
                @Override
                public void run() {
                    try {
                        super.run();
                        String line;
                        while (true) {
                            try {
                                if (!((line = controllerIn.readLine()) != null)) break;
                            } catch (IOException e) {
                                e.printStackTrace();
                                continue;
                            }
                            //Splitting request to command and arguments
                            String[] com = line.split(" ");
                            String command = com[0];

                            System.out.println(BLUE + "[INFO]:Received command " + line + " from controller");
                            System.out.print(WHITE);
                            handleCommand(controller, command, com, controllerOut);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }.start();

            //Creating client listening thread
            new Thread(){
                @Override
                public void run() {
                    super.run();
                    for (;;) {
                        try {
                            Socket client = ss.accept();
                            ss.setSoTimeout(timeout);
                            new Thread(new DStoreServiceThread(client)).start();
                        } catch (Exception e) {
                            //                    System.out.println("error::::::::::::::::::: "+e);
                        }
                    }
                }
            }.start();
        }
        catch (Exception e){
            System.out.println("[ERROR]:Issue while establishing connection with client");
            e.printStackTrace();
        }
    }

    private static void handleCommand(Socket controller, String command, String[] args, PrintWriter controllerOut) {

        File folder = new File(file_folder);

        File[] listOfFiles = folder.listFiles();

        switch (command){
            case "LIST":
                String resp = "LIST";
                for(File file : listOfFiles){
                    resp += " "+file.getName();
                }
                send(controllerOut,resp);

                break;
            case "REMOVE":
                String filename = args[1];

                //If for any reason the file doesn't exist on this dstore, let the controller know
                if(!Arrays.stream(listOfFiles).map(f -> f.getName()).toList().contains(filename)){
                    send(controllerOut,"ERROR_FILE_DOES_NOT_EXIST "+filename);
                }

                //Deleting the file from the list of files
                for(File file : listOfFiles){
                    if(file.getName().equals(filename)){
                        send(controllerOut,"REMOVE_ACK "+file.getName());
                        //Try to delete the file from the dstore
                        try {
                            Files.delete(Path.of(file.getAbsolutePath()));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                }

                listOfFiles = folder.listFiles();

                //Printing the revised list of files on this dstore
                System.out.println("==============");
                System.out.println("NEW FILE LIST:");
                System.out.println("==============");
                int ii = 0;
                for (File f : listOfFiles){
                    System.out.println("["+ii+"] "+f.getName());
                    ii++;
                }
                System.out.println("--------------");
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
                OutputStream outf = client.getOutputStream();

                String line = in.readLine();

                String[] args = line.split(" ");
                String command = args[0];
                System.out.println("Received:" + line);

                File dir = new File(file_folder);
                if (command.equals("STORE")) {
                    String filename = args[1];
                    Integer filesize = Integer.parseInt(args[2]);

                    //Acknowledging readiness to receive file
                    out.println("ACK");
                    out.flush();

                    //Creating a new file with the given filename
                    File outputFile = new File(file_folder, filename);

                    //Create new file even if it exist
                    outputFile.createNewFile();

                    //Receiving data
                    FileOutputStream fout = new FileOutputStream(outputFile);
                    var file = client.getInputStream().readNBytes(filesize);

                    //Writing data to file
                    fout.write(file);

                    //Sending store acknowledgement to controller
                    PrintWriter informController = new PrintWriter(controller.getOutputStream());
                    send(informController, "STORE_ACK " + filename);

                    File[] listOfFiles = dir.listFiles();

                    //Printing the revised list of files on this dstore
                    System.out.println("==============");
                    System.out.println("NEW FILE LIST:");
                    System.out.println("==============");
                    int ii = 0;
                    for (File f : listOfFiles) {
                        System.out.println("[" + ii + "] " + f.getName());
                        ii++;
                    }
                    System.out.println("--------------");
                } else if (command.equals("LOAD_DATA")) {
                    String filename = args[1];

                    File[] listOfFiles = dir.listFiles();
                    for (File file : listOfFiles) {
                        if (file.getName().equals(filename)) {
                            //Send file contents to the client requesting them
                            outf.write(Files.readAllBytes(Path.of(file.getAbsolutePath())));
                            System.out.println("Sending file " + file.getName() + " to client on port " + client.getPort());
                            client.close();
                            break;
                        }
                    }
                    //If file doesn't exist => close the socket with the client
                    client.close();
                }
            } catch (FileNotFoundException ex) {
                ex.printStackTrace();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
    private static void send(PrintWriter out, String message) {
        System.out.println(YELLOW+"SENDING: "+message+WHITE);
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
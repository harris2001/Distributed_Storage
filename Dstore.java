import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLOutput;

public class Dstore {
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

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(helloSocket.getInputStream()));
            PrintWriter out = new PrintWriter(helloSocket.getOutputStream());

            out.println("DSTORE " + port);
            out.flush();
            System.out.println("[INFO]:Sending CONNECT " + port);
//            ServerSocket ss = new ServerSocket(port);
            while(true){
                try{
//                    Socket client = ss.accept();
//                    new Thread(new DStoreServiceThread(client)).start();
                    for(int i=0;i<10;i++){
                        out.println("LOAD test"); out.flush();
                        System.out.println("TCP message "+i+" sent");
                        Thread.sleep(1000);
                        while(true){

                            String line;
                            while((line =in.readLine())!=null) {
                                System.out.println(line);
                            }
                        }
                    }

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

                if (command.equals("ACK")) {
                    controller = client;
                    System.out.println("[INFO]:Established connection with controller");
                }
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

                    for(File file : listOfFiles){
                        if(file.isFile() && file.getName().equals(filename)){

                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

//            File outputFile = new File(args[2]);
//            FileOutputStream outf = new FileOutputStream(outputFile);
//            try{
//                Socket socket = new Socket(args[0],4323);
//                OutputStream out = socket.getOutputStream();
//                InputStream in = socket.getInputStream();
//                out.write(("get"+" "+args[3]+" ").getBytes());
//                StringBuilder response = new StringBuilder();
//                byte[] respbuf = new byte[256];
//                int resplen = in.read(respbuf);
//
//                for(int i=0; i<resplen; i++){
//                    response.append(Character.toString(respbuf[i]));
//                }
//                if(response.toString().equals("NOT_FOUND")){
//                    System.out.println("File requested not found");
//                    return;
//                }
//                if(response.toString().equals("FILE_FOUND")){
//                    System.out.println("File found, sending content");
//                }
//                byte[] buf = new byte[1000]; int buflen;
//
//                while ((buflen=in.read(buf)) != -1){
//                    System.out.print("*");
//                    outf.write(buf,0,buflen);
//                }
//                out.close();
//                in.close();
//            }catch(Exception e){System.out.println("error"+e);}
//            System.out.println();
//            outf.close();
//        } else
//            System.out.println("unrecognised command");
}

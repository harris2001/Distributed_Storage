import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class Controller {

    //Controller's listening port
    private static int cport;
    //Replication factor for files
    private static int R;
    //Failure detection timeout (in milliseconds)
    private static int timeout;
    //Period to start re-balance operation (in seconds)
    private static int rebalance_period;

    //Keeps a list of the ports of all connected dstores
    private static ArrayList<Integer> dstores;

    //Keeps track of all dstores and the files they store
    private static HashMap<String, ArrayList<Integer>> storage;

    private static final String BLACK = "\u001B[30m";
    private static final String RED = "\u001B[31m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String BLUE = "\u001B[34m";
    private static final String PURPLE = "\u001B[35m";
    private static final String CYAN = "\u001B[36m";
    private static final String WHITE = "\u001B[37m";

    //Possible progress states
    private enum Progress{
        store_in_progress,
        store_complete,
        remove_in_progress,
        remove_complete
    }

    //Used to indicate the progress of an operation
    private static HashMap<String,Progress> index;

    /**
     * The main function is called as soon as the Controller is started
     * @param args are the command line arguments passed in the Controller (Explanations above)
     */
    public static void main(String[] args) {
        cport = Integer.parseInt(args[0]);
        R = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        rebalance_period = Integer.parseInt(args[3]);

        storage = new HashMap<>();
        dstores = new ArrayList<>();

        try{
            ServerSocket ss = new ServerSocket(cport);
            for(;;){
                try{
                    System.out.println("Waiting for connection...");
                    Socket client = ss.accept();
                    new Thread(new FileServiceThread(client)).start();
                } catch(Exception e){System.out.println("error1 "+e);}
            }
        }catch(Exception e){System.out.println("error2 "+e);}

    }

//    static ArrayList<String> listFiles(Socket socket){
//        try {
//            PrintWriter out = new PrintWriter(socket.getOutputStream());
//            out.println("LIST");
//            out.flush();
//        } catch (IOException e) {
//            System.out.println("[ERROR]:Can't list send LIST command");
//            e.printStackTrace();
//        }
//        return null;
//    }

    /////////////////////////// TODO ///////////////////////////
//    private ArrayList<Integer> rebalancing(){
//        ArrayList<Integer>ports = new ArrayList<Integer>();
//
//        for(Socket socket : dstores){
//            int fileAdded = storage.get(socket).size()+1;
//            int factor = R*F/dstores.size();
//            if(fileAdded>=Math.floor(factor) && fileAdded<=Math.ceil(factor)){
//                ports.add(socket.getPort());
//            }
//        }
//        return ports;
//    }
    ////////////////////////////////////////////////////////////

    public static ArrayList<Integer> selectRDstores(){
        int i=0;
        ArrayList<Integer>ports = new ArrayList<>();
        //Gets all dstores that are active and sorts them based on the number of files they contain
        //Finally it updates the storage object

        storage = storage.entrySet().stream()
                .filter(i1->dstores.contains(i1.getKey()))
                .sorted(Comparator.comparing(i1->i1.getValue().size()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, HashMap::new));
//        for(Socket socket : dstores){
//            if(i==R)
//                break;
//            i++;
//        }
        return ports;
    }

    static class FileServiceThread implements Runnable {
        Socket client;
        CountDownLatch doneSignal;
        int connectedPort;

        FileServiceThread(Socket c){
            client = c;
            System.out.println("[INFO]:Client "+client.getInetAddress()+" connected at port "+client.getPort());
        }

        @Override
        public void run() {
            try {
                while(!client.isClosed()) {
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(client.getInputStream()));
                    PrintWriter out = new PrintWriter(client.getOutputStream());

                    //Reading request
                    String line;
                    while ((line = in.readLine()) != null) {

                        //Splitting request to command and arguments
                        String[] args = line.split(" ");
                        String command = args[0];

                        if (command.equals("DSTORE")) {

                            int dstorePort = Integer.parseInt(args[1]);
                            //Saving the port because when the socket is closed, we can't
                            //ask for it (needed to delete dstore from dstores)
                            this.connectedPort = dstorePort;

                             //Adding new dstore in dstores array
                            dstores.add(dstorePort);

                        } else if (command.equals("STORE")) {
                            String filename = args[1];
                            Integer filesize = Integer.parseInt(args[2]);
                            //If file already exist, notify client
                            if (index.get(filename) != null) {
                                out.println("ERROR_FILE_ALREADY_EXISTS");
                                out.flush();
                                continue;
                            }

                            //Updating index
                            index.put(filename, Progress.store_in_progress);

                            //Adding file to the storage
                            storage.put(filename,new ArrayList<>());

                            //If there are not enough DStores (less than R), notify client
                            if (dstores.size() < R) {
                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                                out.flush();
                                continue;
                            }
                            //Sending the list of ports to the client
                            ArrayList<Integer> ports = selectRDstores();
                            out.println("STORE_TO " + getString(ports));
                            out.flush();

                            //Gives timeout number of milliseconds to all R dstores to respond with an ACK
                            doneSignal = new CountDownLatch(R);
                            doneSignal.await(timeout, TimeUnit.MILLISECONDS);
                        } else if (command.equals("STORE_ACK")) {
                            String filename = args[1];

                            //Assuming that there are files in the storage
                            assert (!storage.isEmpty());

                            //Adding dstore to the list of dstores that contain the file
                            storage.get(filename).add(this.connectedPort);

                            //Decrease doneSignal counter
                            doneSignal.countDown();
                            //When store operations are performed by all R dstores
                            if (doneSignal.getCount() == 0) {
                                //Change the index of the file to store_completed
                                index.put(filename, Progress.store_complete);
                                //Inform the client
                                out.println("STORE_COMPLETED");
                            }
                        } else if (command.equals("LOAD")) {
                            String filename = args[1];
//                            Socket port1 = storage.get(filename).getDstores().get(0);
//                            responseWriter.println("LOAD_FROM "+port1.getPort());

                        } else if (command.equals("LIST")) {
                            //                        out.println("LIST"); //TODO return filenames saved on this dstore
                        } else {
                            System.out.println("[WARNING]: Command" + command + " not found");
                        }
                    }
                    client.close();
                }
                //Logging connection drop
                System.out.println("[INFO]:Connection with client at port "+client.getPort()+" was dropped");

                //Removing dstore from dstores if connection is dropped
                for(Integer dstorePort : dstores){
                    if(dstorePort==this.connectedPort){
                        dstores.remove(dstorePort);
                        break;
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        private String getString(ArrayList<Integer> ports) {
            String ans = "";
            for(Integer port : ports){
                ans+=port+" ";
            }
            return ans;
        }
    }
}

// For FTP transmissions

                //            InputStream in = client.getInputStream();
                //
                //            System.out.println("Trying1");
                //            byte[] buf = new byte[1000]; int buflen;
                //            buflen=in.read(buf);
                //            System.out.println("Trying");
                //            String firstBuffer=new String(buf,0,buflen);
                //            int firstSpace=firstBuffer.indexOf(" ");
                //            String command=firstBuffer.substring(0,firstSpace);
                //            System.out.println("command "+command);
                //            if(command.equals("put")){
                //                int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
                //                String fileName=
                //                        firstBuffer.substring(firstSpace+1,secondSpace);
                //                System.out.println("fileName "+fileName);
                //                File outputFile = new File(fileName);
                //                FileOutputStream out = new FileOutputStream(outputFile);
                //                out.write(buf,secondSpace+1,buflen-secondSpace-1);
                //                while ((buflen=in.read(buf)) != -1){
                //                    System.out.print("*");
                //                    out.write(buf,0,buflen);
                //                }
                //                in.close(); client.close(); out.close();
                //            } else
                //            if(command.equals("get")){
                //                int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
                //                String fileName=
                //                        firstBuffer.substring(firstSpace+1,secondSpace);
                //                System.out.println("fileName "+fileName);
                //                File inputFile = new File(fileName);
                //                FileInputStream inf = new FileInputStream(inputFile);
                //                OutputStream out = client.getOutputStream();
                //                out.write("FILE_FOUND".getBytes());
                //                out.flush();
                //                while ((buflen=inf.read(buf)) != -1){
                //                    System.out.print("*");
                //                    out.write(buf,0,buflen);
                //                }
                //                in.close(); inf.close(); client.close(); out.close();
                //
                //            } else
                //                System.out.println("unrecognised command");
//                }
//            } catch (IOException e) {
//            OutputStream out = null;
//            try {
//                out = client.getOutputStream();
//                byte[] buf = "NOT_FOUND".getBytes();
//                Thread.sleep(1000);
//                out.write(buf);
//            } catch (IOException ex) {
//                ex.printStackTrace();
//            } catch (InterruptedException ex) {
//                ex.printStackTrace();
//            }


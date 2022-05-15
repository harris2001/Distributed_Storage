import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.concurrent.ThreadLocalRandom;


public class Controller {

    private static final String BLACK = "\u001B[30m";
    private static final String RED = "\u001B[31m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String BLUE = "\u001B[34m";
    private static final String PURPLE = "\u001B[35m";
    private static final String CYAN = "\u001B[36m";
    private static final String WHITE = "\u001B[37m";

    //Controller's listening port
    private static int cport;
    //Replication factor for files
    private static int R;
    //Failure detection timeout (in milliseconds)
    private static int timeout;
    //Period to start re-balance operation (in seconds)
    private static int rebalance_period;

    //Keeps a list of the ports of all connected dstores
    private static HashMap<Integer,Socket> dstores;

    //Keeps track of all dstores and the files they store
    private static HashMap<Integer, ArrayList<String>> storage;

    // Stores the files and the space
    private static HashMap<String, Integer> capacity;

    //Hashmap that stores the latches for each file
    private static HashMap<String, CountDownLatch> latches;


    //Possible progress states
    private enum Progress{
        store_in_progress,
        store_complete,
        remove_in_progress,
        remove_complete
    }

    //Used to indicate the progress of an operation
    private static Map<Object, Object> index;

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
        dstores = new HashMap<>();
        capacity = new HashMap<>();
        latches = new HashMap<>();
        index = Collections.synchronizedMap(new HashMap<>());


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

    private static void newLatch(String filename){
        latches.put(filename, new CountDownLatch(R));
        System.out.println(WHITE+"[INFO]: New latch created");
        boolean started = false;
        try {
            started = latches.get(filename).await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(!started){
            System.out.println(RED+"[ISSUE]: Didn't received all "+R+" acknowledgements from Dstores in time");
            System.out.print(WHITE);
        }
    }

//    static ArrayList<Integer> listFiles(String filename){
//
//        return ports;
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
//        }
//        return ports;
//    }
    ////////////////////////////////////////////////////////////

    public static ArrayList<Integer> selectRDstores(){
        ArrayList<Integer>ports = new ArrayList<>();

        //Gets all dstores that are active and sorts them based on the number of files they contain
        //Finally it updates the storage object

        storage = storage.entrySet().stream()
                .filter(i1->dstores.containsKey(i1.getKey()))
                .sorted(Comparator.comparing(i1->i1.getValue().size()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1, HashMap::new));

        Iterator<Integer>iter = storage.keySet().iterator();

        int i=0;
        while(iter.hasNext() && i<R){
            System.out.println(RED+iter.next());
            ports.add(iter.next());
            i++;
        }
        while(i<R){
            Iterator<HashMap<Integer,Socket>> it;
            for(Integer port: dstores.keySet()){
                if(!ports.contains(port) && i<R){
                    ports.add(port);
                    i++;
                }
            }
        }

        return ports;
    }

    static class FileServiceThread implements Runnable {
        Socket client;
        int connectedPort;

        FileServiceThread(Socket c){
            client = c;
            System.out.println(GREEN+"[INFO]:Client "+client.getInetAddress()+" connected at port "+client.getPort());
            System.out.print(WHITE);
        }

        @Override
        public void run() {
            try {
                while (!client.isClosed()) {
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(client.getInputStream()));
                    PrintWriter out = new PrintWriter(client.getOutputStream());

                    //Reading request
                    String line;
                    while ((line = in.readLine()) != null) {

                        //Splitting request to command and arguments
                        String[] args = line.split(" ");
                        String command = args[0];

                        System.out.println(BLUE + "[INFO]:Received command " + line + " from client on port " + client.getPort());
                        System.out.print(WHITE);
                        handleRequest(client, command, args, out);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void handleRequest(Socket client, String command, String args[], PrintWriter out) {
            switch (command) {
                case "JOIN":
                    int dstorePort = Integer.parseInt(args[1]);
                    handleDstore(dstorePort);
                    break;
                case "STORE":
                    String filename = args[1];
                    Integer filesize = Integer.parseInt(args[2]);
                    //If file already exist, notify client
                    if (index.get(filename) != null) {
                        send(out, "ERROR_FILE_ALREADY_EXISTS");
                        return;
                    }

                    //Updating index
                    index.put(filename, Progress.store_in_progress);

                    //Storing file and its capacity
                    capacity.put(filename, filesize);

                    //Sending the list of ports to the client
                    ArrayList<Integer> ports = selectRDstores();

                    //If there are not enough DStores (less than R), notify client
                    if (ports.size() < R) {
                        send(out, "ERROR_NOT_ENOUGH_DSTORES");
                        return;
                    }

                    //Otherwise specify the ports to store the file to
                    send(out, "STORE_TO " + getString(ports));

                    //Gives timeout number of milliseconds to all R dstores to respond with an ACK
                    newLatch(filename);

                    //When store operations are performed by all R dstores
                    if (latches.get(filename).getCount() == 0) {
                        //Change the index of the file to store_completed
                        index.put(filename, Progress.store_complete);
                        //Inform the client
                        send(out, "STORE_COMPLETE");
                    }
                    break;
                case "STORE_ACK":
                    filename = args[1];

                    //Assuming that there are files in the storage
                    assert (!capacity.isEmpty());


                    ArrayList<String> files = new ArrayList<>();
                    //Adding dstore to the list of dstores that contain the file
                    if (storage.get(this.connectedPort) == null) {
                        files.add(filename);
                        storage.put(this.connectedPort, files);
                    } else {
                        files = storage.get(this.connectedPort);
                        files.add(filename);
                        storage.replace(this.connectedPort, files);
                    }
                    System.out.println(GREEN + "STORAGE NEW CAPACITY: " + storage.size() + " New pair added: (" + filename + "," + this.connectedPort + ")");

                    latches.get(filename).countDown();

                    break;
                case "LOAD":
                    filename = args[1];

                    // If file doesn't exist or if it's still processed inform client
                    if (index.get(filename) == null || index.get(filename).equals(Progress.store_in_progress)) {
                        send(out, "ERROR_FILE_DOES_NOT_EXIST");
                    }

                    //Choose first dstore from the list
                    Iterator<Integer> portIter = storage.keySet().iterator();
                    boolean done = false;
                    while (portIter.hasNext() && done == false) {
                        int port = portIter.next();
                        System.out.println("Storage size: " + storage.get(port).size());
                        for (String file : storage.get(port)) {
                            if (file.equals(filename)) {
                                send(out, "LOAD_FROM " + port + " " + capacity.get(filename));
                                //Operation is done
                                done = true;
                                break;
                            }
                        }
                    }
                    if (done)
                        return;

                    //If there are no more dstores, inform client
                    send(out, "ERROR_NOT_ENOUGH_DSTORES");

                    break;
                case "RELOAD":
                    filename = args[1];
                    int dstore = selectLoadDstore(filename);

                    //If no other dstore contains that file, return ERROR_LOAD message
                    if (dstore == 0)
                        send(out, "ERROR_LOAD");
                    else
                        send(out, "LOAD_FROM " + dstore + " " + capacity.get(filename));
                    break;

                case "LIST":
                    boolean any = false;
                    out.print("LIST");
                    System.out.print(YELLOW + "[INFO]: Sending LIST");
                    if (index.isEmpty()) {
                        out.println();
                        out.flush();
                        System.out.println(WHITE);
                        return;
                    }
                    for (Object name : index.keySet()) {
                        out.print(" " + name);
                        System.out.print(" " + name);
                        any = true;
                    }
                    System.out.println(WHITE);
                    out.println("");
                    if (any) {
                        out.flush();
                    } else {
                        System.out.println(RED + "[WARNING]: Sending ERROR_NOT_ENOUGH_DSTORES");
                        System.out.print(WHITE);
                        out.println("ERROR_NOT_ENOUGH_DSTORES");
                        out.flush();
                    }
                    break;
                case "REMOVE":
                    filename = args[1];

                    //If there are not enough DStores (less than R), notify client
                    if (dstores.size() < R) {
                        send(out, "ERROR_NOT_ENOUGH_DSTORES");
                        return;
                    }

                    // If file doesn't exist or if it's still processed inform client
                    if (index.get(filename) == null || index.get(filename).equals(Progress.store_in_progress)) {
                        send(out, "ERROR_FILE_DOES_NOT_EXIST");
                    }

                    index.put(filename,Progress.remove_in_progress);
                    for (int port : storage.keySet()){
                        files = storage.get(port);
                        if(files.contains(filename)){
                            PrintWriter outDstore = null;
                            try {
                                outDstore = new PrintWriter(dstores.get(port).getOutputStream());
                            } catch (IOException e) {
                                //Cannot connect to Dstore => ignore
                                e.printStackTrace();
                            }
                            send(outDstore,"REMOVE "+filename);
                        }
                    }

                    //Gives timeout number of milliseconds to all R dstores to respond with an ACK
                    newLatch(filename);

                    //When remove operations are performed by all R dstores
                    if (latches.get(filename).getCount() == 0) {
                        //Change the index of the file to remove_complete
                        index.put(filename, Progress.remove_complete);
                        //Inform the client
                        send(out, "REMOVE_COMPLETE");
                    }
                    break;
                case "REMOVE_ACK":
                    filename = args[1];

                    //Assuming that there are files in the storage
                    assert (!capacity.isEmpty());

                    //Removing the (dstore-file) association form the storage
                    storage.remove(this.connectedPort, filename);

                    System.out.println(GREEN + "STORAGE NEW CAPACITY: " + storage.size() + " New pair added: (" + filename + "," + this.connectedPort + ")");

                    //Counting down the latch for that file
                    latches.get(filename).countDown();

                    break;
                case "ERROR_FILE_DOES_NOT_EXIST":
                    filename = args[1];
                    //if the Dstore doesn't have the file, then remove it from the storage ArrayList
                    storage.remove(dstores.get(client.getPort()),filename);
                default:
                    System.out.println(RED + "[WARNING]: Command" + command + " not found");
                    System.out.print(WHITE);
                    break;
            }
        }

        private int selectLoadDstore(String filename) {
            ArrayList<Integer> containFile = new ArrayList<Integer>();
            int selected = 0;
            for (int dstore : storage.keySet()) {
                for (String file : storage.get(dstore)) {
                    if (file.equals(filename)) {
                        containFile.add(dstore);
                    }
                }
            }
            //Returns 0 if no dstore contains the file or a random dstore among the ones that contain the file
            return containFile.size()==0 ? 0 : containFile.get(ThreadLocalRandom.current().nextInt(0, containFile.size()));
        }

        private void handleDstore(int dstorePort){
            //Saving the port because when the socket is closed, we can't
            //ask for it (needed to delete dstore from dstores)
            this.connectedPort = dstorePort;

            //Adding new dstore in dstores array
            dstores.put(dstorePort,client);
            System.out.println("DSTORES: "+dstores.size());
        }

        private void send(PrintWriter out, String message) {
            out.println(message);
            out.flush();
            System.out.println(YELLOW+"[INFO]: Sending "+message);
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
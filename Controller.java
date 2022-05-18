import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Controller {

    protected static final String BLACK = "\u001B[30m";
    protected static final String RED = "\u001B[31m";
    protected static final String GREEN = "\u001B[32m";
    protected static final String YELLOW = "\u001B[33m";
    protected static final String BLUE = "\u001B[34m";
    protected static final String PURPLE = "\u001B[35m";
    protected static final String CYAN = "\u001B[36m";
    protected static final String WHITE = "\u001B[37m";

    //Controller's listening port
    private static int cport;
    //Replication factor for files
    public static int R;
    //Failure detection timeout (in milliseconds)
    private static int timeout;
    //Period to start re-balance operation (in seconds)
    private static int rebalance_period;

    //Keeps a list of the ports of all connected dstores
    protected static HashMap<Integer,Socket> dstores;

    //Keeps track of all dstores and the files they store
    protected static HashMap<Integer, ArrayList<String>> storage;

    // Stores the files and the space
    private static HashMap<String, Integer> capacity;

    //Hashmap that stores the latches for each file
    private static HashMap<String, LatchSocketPair> latches;

    //This is a priority queue to save the incoming commands
    private static Queue<String[]> commandsQueue;

    //Rebalance thread
    private static Rebalance rebalance;

    //Is set to true when the rebalancing operator has the control
    protected static AtomicBoolean isRebalancing = new AtomicBoolean(false);

    public static ConcurrentLinkedQueue<Boolean> pendingOperations;


//    private Controller instance = this;

    public void rebalance_finished() {
        System.out.println(RED+"<<<<<<<<<<<<<REBLANCE FINISHED"+WHITE);
        synchronized (isRebalancing) {
            isRebalancing.set(false);
            isRebalancing.notifyAll();
        }
    }

    public void rebalance() {
        System.out.println(RED+">>>>>>>>>>>>>REBALANCING NOW"+WHITE);
        synchronized (isRebalancing) {
            isRebalancing.set(true);
            System.out.println(GREEN+"Setting isRebalancing to true: "+isRebalancing.get()+WHITE);
            isRebalancing.notifyAll();
        }
    }

    //Possible progress states
    private enum Progress{
        store_in_progress,
        store_complete,
        remove_in_progress,
        remove_complete
    }

    //Used to indicate the progress of an operation
    private static Map<String, Progress> index;

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
        commandsQueue = new LinkedList<>();
        pendingOperations = new ConcurrentLinkedQueue<>();

        try{
            ServerSocket ss = new ServerSocket(cport);
            rebalance = new Rebalance(new Controller(),rebalance_period);
            rebalance.start();

            for(;;){
                try{
                    System.out.println("Waiting for connection...");
                    Socket client = ss.accept();
                    new Thread(new FileServiceThread(client)).start();
                } catch(Exception e){System.out.println("error1 "+e);}
            }
        }catch(Exception e){System.out.println("error2 "+e);}

    }

    private static void newLatch(String filename, Socket client, PrintWriter out){
        CountDownLatch latch = new CountDownLatch(R);
        latches.put(filename, new LatchSocketPair(latch,out));
        System.out.println(WHITE+"[INFO]: New latch created");
        boolean started = false;
        try {
            started = latches.get(filename).getLatch().await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(!started){
            System.out.println(RED+"[ISSUE]: Didn't received all "+R+" acknowledgements from Dstores in time");
            System.out.print(WHITE);
            pendingOperations.poll();
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

        //Adding empty dstores to the list of available dstores
        int i=0;
        for(Integer port: dstores.keySet()){
            //If it's not in storage but it's still active add it
            if(!storage.keySet().contains(port) && i<R){
                ports.add(port);
                System.out.println(RED + "Empty: " +port + WHITE);
                i++;
            }
        }

        //Complete the list with the sorted list of dstores
        storage.entrySet().stream()
                //Get the active dstores
                .filter(i1 -> dstores.containsKey(i1.getKey()))
                //That are not contained in the ports list already
                .filter(i1 -> !ports.contains(i1.getKey()))
                //And sort them based on the amount of files they store
                .sorted(Comparator.comparing(i1 -> i1.getValue().size()))
                .forEach(i1 -> {
                    int port = i1.getKey();
                    System.out.println(RED + ":::::::::::::::::" +port + WHITE);
                    ports.add(port);
                });
        //This is to be returned
        ArrayList<Integer> res = new ArrayList<Integer>();
        i = 0;
        //Return only the first R dstores
        while(i<R && ports.size()>i){
            res.add(ports.get(i));
            i++;
        }

        return res;
    }

    static class FileServiceThread implements Runnable {
        Socket client;
        int connectedPort;

        FileServiceThread(Socket c){
            client = c;
            System.out.println(GREEN+"[INFO]:Client "+client.getInetAddress()+" connected at port "+client.getPort()+WHITE);
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

                        System.out.println(BLUE + "[INFO]:Received command " + line + " from client on port " + client.getPort());
                        System.out.print(WHITE);

//                        handleRequest(client,args[0],args,out);
                        //Adding command to the queue
                        commandsQueue.add(args);

                        synchronized (isRebalancing) {
                            if (isRebalancing.get() == true) {
                                System.out.println("Waiting");
                                isRebalancing.wait();
                            }
                        }

                            if (isRebalancing.get() == false) {
                                while (!commandsQueue.isEmpty()) {
                                    String[] top = commandsQueue.poll();
                                    System.out.print(GREEN+"::::::::"+WHITE);
                                    for (String s : top) {
                                        System.out.print(RED + s + " ");
                                    }
                                    System.out.println(WHITE);
                                    handleRequest(client, top[0], top, out);
                                }
                            }
//                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void handleRequest(Socket client, String command, String args[], PrintWriter out) throws IOException, InterruptedException {
            switch (command) {
                case "JOIN":
                    rebalance = new Rebalance(new Controller(), rebalance_period);
                    int dstorePort = Integer.parseInt(args[1]);
                    handleDstore(dstorePort);
                    break;
                case "STORE":
                    String filename;
                    //Controller gets the control
                    pendingOperations.add(true);

                    filename = args[1];
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
                    newLatch(filename, client, out);

                    //When store operations are performed by all R dstores
                    if (latches.get(filename).getLatch().getCount() == 0) {
                        //Change the index of the file to store_completed
                        index.put(filename, Progress.store_complete);
                        //Inform the client
                        send(out,"STORE_COMPLETE");

                        //After the store is finished we remove the operation from the queue
                        pendingOperations.poll();
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
                    System.out.println(GREEN + "STORAGE NEW CAPACITY: " + storage.size() + " New pair added: (" + filename + "," + this.connectedPort + ")"+WHITE);

                    latches.get(filename).getLatch().countDown();
//                    System.out.println("QQQQQQ: "+latches.get(filename).getLatch().getCount());

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
                    //If there are not enough DStores (less than R), notify client
                    if (dstores.size() < R) {
                        System.out.println(RED + dstores.size() + " " + R + WHITE);
                        send(out, "ERROR_NOT_ENOUGH_DSTORES");
                        return;
                    }

                    out.print("LIST");

                    System.out.print(YELLOW + "[INFO]: Sending LIST");
                    if (index.isEmpty()) {
                        out.println();
                        out.flush();
                        System.out.println(WHITE);
                        return;
                    }
                    for (String name : index.keySet()) {
                        if (index.get(name) == Progress.store_complete) {
                            out.print(" " + name);
                            System.out.print(" " + name);
                            any = true;
                        }
                    }
                    System.out.println(WHITE);
                    out.println("");
                    if (any) {
                        out.flush();
                    } else {
                        System.out.print(WHITE);
                        out.println("ERROR_NOT_ENOUGH_DSTORES");
                        out.flush();
                    }
                    break;
                case "REMOVE":
                    pendingOperations.add(true);

                    filename = args[1];

                    //Assuming that there are files in the storage
                    assert (!capacity.isEmpty());

                    //If there are not enough DStores (less than R), notify client
                    if (dstores.size() < R) {
                        send(out, "ERROR_NOT_ENOUGH_DSTORES");
                        return;
                    }

                    // If file doesn't exist or if it's still processed inform client
                    if (index.get(filename) == null || index.get(filename).equals(Progress.store_in_progress)) {
                        send(out, "ERROR_FILE_DOES_NOT_EXIST");
                    }

                    index.put(filename, Progress.remove_in_progress);
                    for (int port : storage.keySet()) {
                        files = storage.get(port);
                        if (files.contains(filename)) {
                            PrintWriter outDstore = null;
                            try {
                                outDstore = new PrintWriter(dstores.get(port).getOutputStream());
                                send(outDstore, "REMOVE " + filename);
                            } catch (IOException e) {
                                //Cannot connect to Dstore => ignore
                                e.printStackTrace();
                            }
                        }
                    }

                    //Gives timeout number of milliseconds to all R dstores to respond with an ACK
                    newLatch(filename, client, out);

                    //When remove operations are performed by all R dstores
                    if (latches.get(filename).getLatch().getCount() == 0) {
                        //Change the index of the file to remove_complete
                        index.put(filename, Progress.remove_complete);
                        //Inform the client
                        send(out, "REMOVE_COMPLETE");

                        //After the store is finished we're releasing the control
                        pendingOperations.poll();
                    }
                    break;
                case "REMOVE_ACK":
                    filename = args[1];

                    //Assuming that there are files in the storage
                    assert (!capacity.isEmpty());

                    //Removing the (dstore-file) association form the storage
                    storage.remove(this.connectedPort, filename);

                    System.out.println(GREEN + "STORAGE NEW CAPACITY: " + storage.size() + " New pair added: (" + filename + "," + this.connectedPort + ")"+WHITE);

                    //Counting down the latch for that file
                    latches.get(filename).getLatch().countDown();

                    break;
                case "ERROR_FILE_DOES_NOT_EXIST":
                    filename = args[1];
                    //if the Dstore doesn't have the file, then remove it from the storage ArrayList
                    storage.remove(dstores.get(client.getPort()), filename);
                    PrintWriter outClient = new PrintWriter(latches.get(filename).getWriter());
                    send(outClient, "ERROR_FILE_DOES_NOT_EXIST");
                    break;
                default:
                    System.out.println(RED + "[WARNING]: Command " + command + " not found" + WHITE);
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
            System.out.println(YELLOW+"[INFO]: Sending "+message+WHITE);
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
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    private static ArrayList<Socket> dstores;

    //Keeps track of all dstores and the files they store
    private static HashMap<Integer, ArrayList<String>> storage;

    // Stores the files and the space
    private static HashMap<String, Integer> capacity;


    //Possible progress states
    private enum Progress{
        store_in_progress,
        store_complete,
        remove_in_progress,
        remove_complete
    }

    //Used to indicate the progress of an operation
    private static HashMap<String,Progress > index;

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
        capacity = new HashMap<>();
        index = new HashMap<>();


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
//            }
//        }
//        return ports;
//    }
    ////////////////////////////////////////////////////////////

    public static ArrayList<Integer> selectRDstores(){
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

        Iterator<Integer>iter = storage.keySet().iterator();

        int i=0;
        while(iter.hasNext() && i<R){
            System.out.println(RED+iter.next());
            ports.add(iter.next());
            i++;
        }
        while(i<R){
            for(Socket port: dstores){
                if(!ports.contains(port.getPort())){
                    ports.add(port.getPort());
                }
            }
            i++;
        }

        return ports;
    }

    static class FileServiceThread implements Runnable {
        Socket client;
        CountDownLatch doneSignal;
        int connectedPort;

        FileServiceThread(Socket c){
            client = c;
            System.out.println(GREEN+"[INFO]:Client "+client.getInetAddress()+" connected at port "+client.getPort());
            System.out.print(WHITE);
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

                        System.out.println(BLUE+"[INFO]:Received command "+line+" from client on port "+client.getPort());
                        System.out.print(WHITE);
                        if (command.equals("DSTORE")) {

                            int dstorePort = Integer.parseInt(args[1]);
                            //Saving the port because when the socket is closed, we can't
                            //ask for it (needed to delete dstore from dstores)
                            this.connectedPort = dstorePort;

                            Thread.sleep(500);
                            //Sending Acknowledgement message and opening a dedicated connection to dstore

                            Socket dstoreSocket = new Socket(client.getInetAddress(), dstorePort);
                            PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream());

                            dstoreOut.println("ACK_DSTORE");
                            dstoreOut.flush();
                            System.out.println("[INFO]:Sending ACK "+dstoreSocket.isClosed());
                            System.out.println("[INFO]:Established connection with Dstore at port " + args[1]);

                            BufferedReader dstoreIn = new BufferedReader(
                                    new InputStreamReader(dstoreSocket.getInputStream()));
                            String dstoreResp;
                            while((dstoreResp = dstoreIn.readLine())!=null){
                                System.out.println("RECEIVED "+dstoreResp);
                            }

                            //Adding new dstore in dstores array
                            dstores.add(dstoreSocket);
                            System.out.println("DSTORES: "+dstores.size());

                        } else if (command.equals("STORE")) {
                            String filename = args[1];
                            Integer filesize = Integer.parseInt(args[2]);
                            //If file already exist, notify client
                            if (index.get(filename) != null) {
                                send(out,"ERROR_FILE_ALREADY_EXISTS");
                                continue;
                            }

                            //Updating index
                            index.put(filename, Progress.store_in_progress);

                            //Storing file and its capacity
                            capacity.put(filename,filesize);

                            //Sending the list of ports to the client
                            ArrayList<Integer> ports = selectRDstores();

                            //If there are not enough DStores (less than R), notify client
                            if(ports.size()<R){
                                send(out,"ERROR_NOT_ENOUGH_DSTORES");
                                continue;
                            }

                            //Otherwise specify the ports to store the file to
                            send(out,"STORE_TO " + getString(ports));
                            out.flush();

                            //Gives timeout number of milliseconds to all R dstores to respond with an ACK
                                doneSignal = new CountDownLatch(R);
                            doneSignal.await(timeout, TimeUnit.MILLISECONDS);
                        } else if (command.equals("STORE_ACK")) {
                            String filename = args[1];

                            //Assuming that there are files in the storage
                            assert (!capacity.isEmpty());

                            //Adding dstore to the list of dstores that contain the file
                            if(storage.get(this.connectedPort)==null){
                                storage.put(this.connectedPort,new ArrayList<>());
                                storage.get(this.connectedPort).add(filename);
                            }
                            else {
                                storage.get(this.connectedPort).add(filename);
                            }
                            //Decrease doneSignal counter
                            doneSignal.countDown();
                            //When store operations are performed by all R dstores
                            if (doneSignal.getCount() == 0) {
                                //Change the index of the file to store_completed
                                index.put(filename, Progress.store_complete);
                                //Inform the client
                                send(out,"STORE_COMPLETED");
                            }
                        } else if (command.equals("LOAD")) {
                            String filename = args[1];

                            // If file doesn't exist or if it's still processed inform client
                            if (index.get(filename) == null || index.get(filename).equals(Progress.store_in_progress)) {
                                send(out,"ERROR_FILE_DOES_NOT_EXIST");
                            }

                            //Choose first dstore from the list
                            Iterator<Integer>portIter = storage.keySet().iterator();
                            boolean done = false;
                            while(portIter.hasNext()){
                                for(String file : storage.get(portIter)){
                                    if(file.equals(filename)){
                                        send(out,"LOAD_FROM " + portIter.next() + " " + capacity.get(filename));
                                        //Opeartion is done
                                        done = true;
                                        portIter = null;
                                        break;
                                    }
                                }
                            }
                            if(done)
                                continue;

                            //If there are no more dstores, inform client
                            send(out,"ERROR_NOT_ENOUGH_DSTORES");

                        } else if (command.equals("RELOAD")){
                            String filename = args[1];
                            Iterator<Integer>portIter = storage.keySet().iterator();
                            Boolean get=false;
                            while(portIter.hasNext()){
                                for(String file : storage.get(portIter)){
                                    if(file.equals(filename)){
                                        if(!get){
                                            storage.remove(portIter);
                                            continue;
                                        }
                                        send(out,"LOAD_FROM " + portIter.next() + " " + capacity.get(filename));
                                        portIter = null;
                                        break;
                                    }
                                }
                            }
                            send(out,"ERROR_LOAD");
                        } else if (command.equals("LIST")) {
                            boolean any=false;
                            out.print("LIST");
                            System.out.print(YELLOW+"[INFO]: Sending LIST");
                            if(index.isEmpty()){
                                out.println();
                                out.flush();
                                System.out.println(WHITE);
                                continue;
                            }
                            for(String name : index.keySet()){
                                out.print(" "+name);
                                System.out.print(" "+name);
                                any=true;
                            }
                            System.out.println(WHITE);
                            out.println("");
                            if(any)
                                out.flush();
                            else{
                                System.out.println(RED+"[WARNING]: Sending ERROR_NOT_ENOUGH_DSTORES");
                                System.out.print(WHITE);
                                out.println("ERROR_NOT_ENOUGH_DSTORES");
                                out.flush();
                            }
                        } else {
                            System.out.println(RED+"[WARNING]: Command" + command + " not found");
                            System.out.print(WHITE);
                        }
                    }
                    client.close();
                }
                //Logging connection drop
                System.out.println(PURPLE+"[INFO]:Connection with client at port "+client.getPort()+" was dropped");
                System.out.print(WHITE);
                //Removing dstore from dstores if connection is dropped
                for(Socket dstorePort : dstores){
                    if(dstorePort.getPort()==this.connectedPort){
                        dstores.remove(dstorePort);
                        break;
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
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
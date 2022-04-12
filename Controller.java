import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.lang.Math;
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

    //Keeps a list of all connected dstores
    private static ArrayList<Socket> dstores;
    //Keeps track of the stored files in each dstore
    private static HashMap<Socket,ArrayList<String>>storage;
    //Total number of files stored
    private static int F;

    //Possible progress states
    private enum Progress{
        store_in_progress,
        store_complete,
        remove_in_progress,
        remove_complete;
    };

    //Used to indicate the progress of an operation
    private static Progress index;

    /**
     * The main function is called as soon as the Controller is started
     * @param args are the command line arguments passed in the Controller (Explanations above)
     */
    public static void main(String[] args) {
        cport = Integer.parseInt(args[0]);
        R = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        rebalance_period = Integer.parseInt(args[3]);

        dstores = new ArrayList<>();

//        try {
//
//            Random random = new Random();
//            for(int dstores=0; dstores<10; dstores++) {
//                ArrayList<String> files1 = new ArrayList<>();
//                int filesStored = random.nextInt(10);
//                for (int files=0; files<filesStored; files++) {
//                    files1.add("newFile:"+files+"_dstore:_"+dstores);
//                    System.out.println("Added newFile: newFile:"+files+"_dstore:_"+dstores);
//                }
//                storage.put(new Socket("127.0.0.1", dstores), files1);
//            }
//            for (int i=0; i<storage.size(); i++){
//                System.out.println("Socket "+i+" has size "+storage.get(i).size());
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

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

    public ArrayList<Integer> selectRDstores(){
        int i=0;
        ArrayList<Integer>ports = new ArrayList<Integer>();
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

        FileServiceThread(Socket c){
            client = c;
            System.out.println("[INFO]:Client "+client.getInetAddress()+" connected at port "+client.getPort());
        }

        @Override
        public void run() {
            try {
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(client.getInputStream()));
                //Reading request
                String line;
                while((line =in.readLine())!=null) {
                    //Splitting request to command and arguments
                    String[] args = line.split(" ");
                    String command = args[0];

                    if (command.equals("DSTORE")) {
                        Thread.sleep(5);
                        //Sending Acknowledgement message and opening a dedicated connection to dstore
                        Socket dstoreSocket = new Socket(client.getInetAddress(), Integer.parseInt(args[1]));
                        PrintWriter out = new PrintWriter(dstoreSocket.getOutputStream());
                        out.println("ACK");
                        System.out.println("[INFO]:Sending ACK");
                        out.flush();
                        System.out.println("[INFO]:Established connection with Dstore at port " + args[1]);
                    }
                    if (command.equals("STORE")) {
                        String filename = args[1];
                        String filesize = args[2];
                        //Updating index
                        index = Progress.store_in_progress;

                        //ArrayList<int>ports = selectRDstores();
                    }
                }
            } catch (IOException ex) {
                //Removing dstore from dstores if connection is dropped
                System.out.println("[INFO]:Connection with client at port "+client.getPort()+" was dropped");
                for(Socket socket : dstores){
                    if(client==socket){
                        dstores.remove(client);
                        return;
                    }
                }
//                ex.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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


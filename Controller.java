import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Controller {
    private static int cport;
    private static int R;
    private static int timeout;
    private static int rebalance_period;

    private static ArrayList<File> index;

    public static void main(String[] args) {
        cport = Integer.parseInt(args[0]);
        R = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        rebalance_period = Integer.parseInt(args[3]);

        index = new ArrayList<File>();

        try{
            ServerSocket ss = new ServerSocket(cport);
            for(;;){
                try{
                    System.out.println("Waiting for connection...");
                    Socket client = ss.accept();
                    new Thread(new FileServiceThread(client)).start();
                    System.out.println("client "+client.getInetAddress()+" connected");
                } catch(Exception e){System.out.println("error1 "+e);}
            }
        }catch(Exception e){System.out.println("error2 "+e);}
        System.out.println();
    }

    static class FileServiceThread implements Runnable {
        Socket client;
        FileServiceThread(Socket c){
            client = c;
        }

        @Override
        public void run() {

            try {
                InputStream in = client.getInputStream();

                byte[] buf = new byte[1000]; int buflen;
                buflen=in.read(buf);
                String firstBuffer=new String(buf,0,buflen);
                int firstSpace=firstBuffer.indexOf(" ");
                String command=firstBuffer.substring(0,firstSpace);
                System.out.println("command "+command);
                if(command.equals("put")){
                    int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
                    String fileName=
                            firstBuffer.substring(firstSpace+1,secondSpace);
                    System.out.println("fileName "+fileName);
                    File outputFile = new File(fileName);
                    FileOutputStream out = new FileOutputStream(outputFile);
                    out.write(buf,secondSpace+1,buflen-secondSpace-1);
                    while ((buflen=in.read(buf)) != -1){
                        System.out.print("*");
                        out.write(buf,0,buflen);
                    }
                    in.close(); client.close(); out.close();
                } else
                if(command.equals("get")){
                    int secondSpace=firstBuffer.indexOf(" ",firstSpace+1);
                    String fileName=
                            firstBuffer.substring(firstSpace+1,secondSpace);
                    System.out.println("fileName "+fileName);
                    File inputFile = new File(fileName);
                    FileInputStream inf = new FileInputStream(inputFile);
                    OutputStream out = client.getOutputStream();
                    out.write("FILE_FOUND".getBytes());
                    out.flush();
                    while ((buflen=inf.read(buf)) != -1){
                        System.out.print("*");
                        out.write(buf,0,buflen);
                    }
                    in.close(); inf.close(); client.close(); out.close();

                } else
                    System.out.println("unrecognised command");
            } catch (IOException e) {
                OutputStream out = null;
                try {
                    out = client.getOutputStream();
                    byte[] buf = "NOT_FOUND".getBytes();
                    Thread.sleep(1000);
                    out.write(buf);
                } catch (IOException ex) {
                    ex.printStackTrace();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                e.printStackTrace();
            }
        }
    }
}

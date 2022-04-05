import java.io.*;
import java.net.Socket;

public class Dstore {
    private static int port;
    private static int cport;
    private static int timeout;
    private static String file_folder;

    public static void main(String[] args) throws IOException {
        port = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        file_folder = args[3];

//        if(args[1].equals("put")){
            File inputFile = new File(args[2]);
            FileInputStream in = new FileInputStream(inputFile);
            try{
                Socket socket = new Socket("127.0.0.1",cport);
                OutputStream out = socket.getOutputStream();
//                out.write(("put"+" "+args[3]+" ").getBytes());
//                byte[] buf = new byte[1000]; int buflen;
//                while ((buflen=in.read(buf)) != -1){
//                    System.out.print("*");
//                    out.write(buf,0,buflen);
//                }
                while (true){
                }
            }catch(Exception e){
                System.out.println("error"+e);
                in.close();
            }
            System.out.println();
            in.close();
//        } else
//        if(args[1].equals("get")){
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
}

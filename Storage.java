import java.net.Socket;
import java.util.ArrayList;

public class Storage {
    private int size;
    private ArrayList<Socket> dstores;

    public Storage(int size, ArrayList<Socket> dstores){
        this.size = size;
        this.dstores = dstores;
    }

    public int getSize() {
        return size;
    }
    public void setSize(int size) {
        this.size = size;
    }

    public ArrayList<Socket> getDstores() {
        return dstores;
    }
    public void addDstores(Socket dstores) {
        this.dstores.add(dstores);
    }
}

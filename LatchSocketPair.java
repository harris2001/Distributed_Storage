import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;

public class LatchSocketPair {

    CountDownLatch latch;
    PrintWriter out;

    public LatchSocketPair(CountDownLatch latch, PrintWriter out){
        this.latch = latch;
        this.out = out;
    }

    public CountDownLatch getLatch() {
        return this.latch;
    }
    public PrintWriter getWriter() {
        return this.out;
    }
}

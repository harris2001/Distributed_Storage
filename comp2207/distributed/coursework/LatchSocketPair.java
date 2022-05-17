package comp2207.distributed.coursework;

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

    //Getters and setters
    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public void setSocket(PrintWriter out) {
        this.out = out;
    }

    public CountDownLatch getLatch() {
        return latch;
    }
    public PrintWriter getWriter() {
        return out;
    }
}

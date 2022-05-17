package comp2207.distributed.coursework;

public class Rebalance extends Thread {

    private int period;
    private Controller controller;

    public Rebalance(Controller controller, int period){
        this.controller = controller;
        this.period = period;
    }

    @Override
    public void run() {
        super.run();
        start_rebalancing();
    }

    private void start_rebalancing() {

    }
}

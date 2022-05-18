import javax.naming.ldap.Control;
import java.time.LocalDateTime;

public class Rebalance extends Thread {

    private int period;
    private static Controller controller;

    public Rebalance(Controller controller, int period){
        this.controller = controller;
        this.period = period;
    }

    @Override
    public void run() {
        super.run();
        try {
            start_rebalancing();
        } catch (InterruptedException e) {
            System.out.println(Controller.RED+"New dstore interrupted current rebalancing"+Controller.WHITE);
            controller.rebalance_finished();
            //e.printStackTrace();
        }
    }

    private void start_rebalancing() throws InterruptedException {
//        int i=0;
        while(true){
            //Busy wait
            if(controller.pendingOperations.size()>0 || controller.storage.keySet().size()==0){
//                if(LocalDateTime.now().getSecond()%10==0) {
//                    if(i==1){
//                        System.out.println(controller.pendingOperations.size());
//                        i=0;
//                    }
//                }
//                else{
//                    i=1;
//                }
                continue;
            }
            else{
                controller.rebalance();
                Thread.sleep(3);
                System.out.println(controller.CYAN+"DOING REBALANCING STUFF"+controller.WHITE);
                controller.rebalance_finished();
                Thread.sleep(period);
            }
        }
    }
}

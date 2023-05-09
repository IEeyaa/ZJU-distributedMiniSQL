package components;

public class HeartBeat extends Thread{

    SocketThread thread;

    public HeartBeat(SocketThread thread){
        this.thread = thread;
    }

    @Override
    public void run(){
        while(true){
            try {
                sleep(10000);
                thread.check();
                
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

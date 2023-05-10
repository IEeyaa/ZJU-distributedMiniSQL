package components;

public class HeartBeat extends Thread{

    SocketThread thread;

    public HeartBeat(SocketThread thread){
        this.thread = thread;
    }

    @Override
    public void run(){
        while(thread.running){
            try {
                sleep(22000);
                thread.check();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

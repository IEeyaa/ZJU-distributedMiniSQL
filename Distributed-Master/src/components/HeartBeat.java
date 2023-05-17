package components;

public class HeartBeat extends Thread {

    HeartBeatThread thread;
    boolean running = true;
    long duration;

    public HeartBeat(HeartBeatThread thread, long duration) {
        this.thread = thread;
        this.duration = duration;
    }

    @Override
    public void run() {
        while (running) {
            try {
                sleep(duration);
                thread.heartbeat();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

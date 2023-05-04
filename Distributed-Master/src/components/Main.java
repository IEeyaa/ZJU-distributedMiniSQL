package components;

public class Main {
    public static void main(String args[]){
        try {
            Master master = new Master();
            master.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

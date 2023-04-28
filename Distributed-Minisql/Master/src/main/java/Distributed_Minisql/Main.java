package Distributed_Minisql;

import java.io.IOException;

public class Main {
    public static void main(String args[]){
        try {
            MasterManager mm = new MasterManager();
            mm.startUp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

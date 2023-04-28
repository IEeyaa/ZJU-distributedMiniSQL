package Distributed_Minisql;

import java.awt.desktop.SystemEventListener;
import java.io.IOException;
import java.net.*;
import java.util.Enumeration;

public class SocketManager {
    public ServerSocket serverSocket;
    public TableManager tableManger;
    public SocketManager(int port, TableManager tableManager) throws IOException{
        this.tableManger = tableManager;
        this.serverSocket = new ServerSocket(port);
    }

    // 启动socket服务
    public void startSocketManager() throws IOException, InterruptedException{
        while (true) {
            Thread.sleep(100);
            // 阻塞等待客户端发起连接
            Socket socket = serverSocket.accept();
            // 建立socketThread类子线程并启动
            SocketThread socketThread = new SocketThread(socket,tableManger);
            // 获取客户端socket的IP地址
            String ipAddress = socket.getInetAddress().getHostAddress();
            if(ipAddress.equals("127.0.0.1"))
                ipAddress = getHostAddress();
            System.out.println("新的socket线程"+ipAddress);
            // 新建socketThread线程，并开始监听消息
            Thread thread = new Thread(socketThread);
            tableManger.addSocket(ipAddress,socketThread);
            System.out.println("新的socket 已经加入表");
            thread.start();
            System.out.println("thread start");
        }
    }


    // 返回本机IP地址字符串
    public static String getHostAddress() {
        try{
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()){
                NetworkInterface netInterface = allNetInterfaces.nextElement();
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()){
                    InetAddress ip = addresses.nextElement();
                    if (ip instanceof Inet4Address && !ip.isLoopbackAddress() && !ip.getHostAddress().contains(":")){
                        return ip.getHostAddress();
                    }
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }
}

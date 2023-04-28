package Distributed_Minisql;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;

public class SocketThread implements Runnable {
    private boolean isRunning = false;
    public BufferedReader input = null;
    public BufferedWriter output = null;
    private final TableManager tableManager;
    private final Socket socket;


    // 构造函数，从socket中获取输入输出流
    public SocketThread(Socket socket, TableManager tableManager) throws IOException{
        this.output = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
        this.input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.isRunning = true;
        this.tableManager = tableManager;
        this.socket = socket;
    }

    // 在线程的run函数中，循环地从socket读取一行指令并进行处理，否则阻塞
    @Override
    public void run() {
        String inputStr;
        try{
            while (isRunning){
                Thread.sleep(500);
                inputStr = input.readLine();
                if (inputStr.equals("end"))
                {
                    ;
                }
                else processCommand(inputStr);
            }
        }
        catch (InterruptedException | IOException e) {
            System.out.println("断开连接"+e);
        }
    }
    // 收到客户端创建表指令后调用该函数
    public boolean createTable(String tableName, String command) throws IOException{
        String bss = tableManager.getBestServer();
        String bssCopy = tableManager.getBestServer(bss);
        SocketThread bssIp = tableManager.getSocketThread(bss);
        SocketThread bssIpCopy = tableManager.getSocketThread(bssCopy);
        System.out.println(bss);
        System.out.println(bssCopy);
        System.out.println(bssIp);
        System.out.println(bssIpCopy);
        if (bssIp == null || bssIpCopy == null){
            System.out.println("error");
            output.write("no enough regions!");
            output.newLine();
            output.flush();
            return false;
        }
        // 对选中的两个服务器各发送建表请求
        // 主机发一份
        Socket toMain = new Socket(bss,8080);
        BufferedWriter out = new BufferedWriter(new java.io.OutputStreamWriter(toMain.getOutputStream()));
        BufferedReader in = new BufferedReader(new java.io.InputStreamReader(toMain.getInputStream()));

        out.write("region:" + tableName + ":" + bss + ":" + bssCopy);
        out.newLine();
        out.write("end");
        out.newLine();
        out.flush();
        System.out.println("发送给"+bss);

        out.write("execute:" + command);
        out.newLine();
        out.write("end");
        out.newLine();
        out.flush();

        // 读取失败/成功信息，如果不成功则返回false
        String input = in.readLine();
        System.out.println("input:"+input);

        if (input.contains("error")){
            output.write(input);
            output.newLine();
            output.flush();
            return false;
        }
        output.write(input);
        output.newLine();
        output.flush();

        // 此时备份成功，需要通知主机它的备份机在哪
        System.out.println("备份成功");
        // 关闭对主机的socket
        toMain.close();
        // 在表管理器中添加这张表
        tableManager.addTable(tableName, bss, bssCopy);
        return true;
    }

    // 处理命令，需要根据字符串判断来源socket的种类
    public void processCommand(String cmd) throws IOException{
        System.out.println("收到指令:"+cmd);
        String sourceIp = socket.getInetAddress().getHostAddress();
        String result = "";
        String[] cmds = cmd.split(":");
        switch (cmds.length){
            // 来自客户端的sql语句，只有create table这一种
            case 1:
                String tableName = cmds[0].split(" ")[2];
                // 删除表指令
                if (cmds[0].split(" ")[0].contains("drop")){
                    System.out.println("删除表指令" + tableName);
                    if (tableName.endsWith(";"))
                        tableName = tableName.substring(0, tableName.length()-1);
                    // 判断是否存在这个表
                    if (!tableManager.tableToIp.containsKey(tableName)){
                        System.out.println(tableName);
                        output.write("error:");
                        System.out.println("不存在表"+tableName);
                    }
                    else {
                        System.out.println("向你发送"+tableManager.getIpAddressMain(tableName));
                        Socket toMain = new Socket(tableManager.getIpAddressMain(tableName), 8080);
                        BufferedWriter out = new BufferedWriter(new java.io.OutputStreamWriter(toMain.getOutputStream()));
                        BufferedReader in = new BufferedReader(new java.io.InputStreamReader(toMain.getInputStream()));
                        out.write("execute:"+cmd);
                        out.newLine();
                        out.write("end");
                        out.newLine();
                        out.flush();
                        String ret = in.readLine();
                        if (ret.contains("error")) {
                            System.out.println("删除表失败");
                        }
                        output.write(ret);
                        toMain.close();
                        tableManager.deleteTable(tableName, tableManager.getIpAddressMain(tableName), tableManager.getIpAddressCopy(tableName));
                        System.out.println("删除表成功" + tableName);
                    }
                    output.newLine();
                    output.write("end");
                    output.newLine();
                    output.flush();
                    return;
                }
                // 否则是创建表指令
                // 检查是否已经创建了这张表
                if (tableManager.tableToIp.containsKey(tableName) || tableManager.tableToCopyIp.containsKey(tableName)) {
                    output.write("duplicated");
                    output.newLine();
                    output.write("end");
                    output.newLine();
                    output.flush();
                    System.out.println("重复");
                }
                // 选择最佳Region服务器，发送
                else {
                    boolean f = createTable(tableName, cmd);
                    if (f){
                        System.out.println("创建表成功");
                    }
                    else {
                        System.out.println("创建表失败");
                    }
                }
                break;
            case 2:
                if (cmds[0].equals("search")){
                    System.out.println("查表指令"+cmds[1]);
                    String tableToSearch = cmds[1];
                    if (tableManager.tableToIp.containsKey(tableToSearch)){
                        String gotIp = tableManager.tableToIp.get(tableToSearch);
                        output.write("<ip>:" + tableToSearch + ":" + gotIp);
                        System.out.println("输出"+"<ip>:" + tableToSearch + ":" + gotIp);
                    }
                    else {
                        output.write("<ip>:" + tableToSearch + ":unreachable");
                        System.out.println("输出"+"<ip>:" + tableToSearch + ":unreachable");
                    }
                    output.newLine();
                    output.write("end");
                    output.newLine();
                    output.flush();
                }
                break;
            default:
                break;
        }
    }

}


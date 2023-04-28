package Distributed_Minisql;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.Socket;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.util.List;

public class ZookeeperManager implements Runnable{
    private final TableManager tableManager;

    ZookeeperManager(TableManager t){
        tableManager = t;
    }

    @Override
    public void run() {
        try {
            ZookeeperStart();
        } catch (Exception e) {
            e.printStackTrace();
        }
        while(true){

        }
    }

    public void ZookeeperStart() throws Exception {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        CuratorFramework client = CuratorFrameworkFactory.newClient("172.20.10.3:2181", retryPolicy);
        client.start();
//        client.create().withMode(CreateMode.EPHEMERAL).forPath("/Distributed_Minisql");

        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/Distributed_Minisql", true);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
//                System.out.println("子节点变化了");
                PathChildrenCacheEvent.Type type = pathChildrenCacheEvent.getType();
                if(type.equals(PathChildrenCacheEvent.Type.CHILD_ADDED)){
                    byte[] data = pathChildrenCacheEvent.getData().getData();
                    String ip_address = new String(data);
                    System.out.println(ip_address);
                    ManageRegionAdded(ip_address);
                }
                if(type.equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)){
                    byte[] data = pathChildrenCacheEvent.getData().getData();
                    String ip_address = new String(data);
                    System.out.println(ip_address);
                    ManageRegionRemoved(ip_address);
                }
            }
        });
        pathChildrenCache.start();
    }

    public void ManageRegionAdded(String ip_address) throws IOException{
        System.out.println("新节点加入"+ip_address);
        tableManager.addServer(ip_address);
    }

    public void ManageRegionRemoved(String ip_address){
        System.out.println("节点断开连接:"+ip_address);
        List<String> AllTable = tableManager.ipToTables.get(ip_address);
        tableManager.ipToTables.remove(ip_address);
        tableManager.serverList.remove(ip_address);
        tableManager.ipToSocket.remove(ip_address);
        // 遍历所有该ip的服务器存在的表，分主本和副本情况考虑，选择另一台服务器用于
        for(String table: AllTable){
            System.out.println("备份表格: "+table);
            // 选出的用于存储信息的新服务器
            String BestServer;
            boolean ismain = false;
            String copyRegion = "", mainRegion="";
            // 主机挂了
            if (tableManager.tableToIp.get(table).equals(ip_address)){
                copyRegion  = tableManager.tableToCopyIp.get(table);
                ismain = true;
                BestServer = tableManager.getBestServer(tableManager.getIpAddressCopy(table));
                tableManager.tableToIp.put(table, BestServer);
            }
            // 副本机挂了
            else if(tableManager.tableToCopyIp.get(table).equals(ip_address)){
                mainRegion = tableManager.tableToIp.get(table);
                BestServer = tableManager.getBestServer(tableManager.getIpAddressMain(table));
                tableManager.tableToCopyIp.put(table, BestServer);
            }
            else {
                System.out.println("此table不存在IP，错误！");
                return;
            }
            if (BestServer.equals("")){
                System.out.println("region不够了");
                return;
            }

            /* 向新选中的主机传送复制该table的消息 */
            // 新选中的主机：BestServer String table
            try {
                // 判断原来的机器是主机还是副本机
                if (ismain){
                    System.out.println("main"+table+copyRegion+BestServer);
                    Socket toMain = new Socket(copyRegion, 8080);
                    BufferedWriter out = new BufferedWriter(new java.io.OutputStreamWriter(toMain.getOutputStream()));
                    BufferedReader in = new BufferedReader(new java.io.InputStreamReader(toMain.getInputStream()));
                    // 和主Region通知，它的备份是谁
                    Socket toBestServer = new Socket(BestServer, 8080);
                    BufferedWriter outb = new BufferedWriter(new java.io.OutputStreamWriter(toBestServer.getOutputStream()));
                    outb.write("region:" + table + ":" + BestServer + ":" + copyRegion);
                    outb.newLine();
                    outb.write("end");
                    outb.newLine();
                    outb.flush();
                    toBestServer.close();

                    out.write("move:" + table + ":" + BestServer);
                    out.newLine();
                    out.write("end");
                    out.newLine();
                    out.flush();
                    // 读取成功/失败信息
                    String msg = in.readLine();
                    // 除去接收到的end
                    in.readLine();
                    if (msg.contains("suc")){
                        System.out.println("成功转移备份");
                    }
                    else {
                        System.out.println("备份错误");
                    }
                    toMain.close();
                }
                else {
                    System.out.println("copy"+table+mainRegion+BestServer);
                    Socket toCopy = new Socket(mainRegion,8080);
                    BufferedWriter outc = new BufferedWriter(new java.io.OutputStreamWriter(toCopy.getOutputStream()));
                    BufferedReader inc = new BufferedReader(new java.io.InputStreamReader(toCopy.getInputStream()));
                    // 通知主Region它的副本机是谁
                    outc.write("region:" + table + ":" + mainRegion + ":" + BestServer);
                    outc.newLine();
                    outc.write("end");
                    outc.newLine();
                    outc.flush();

                    outc.write("copy:" + table + ":" + BestServer);
                    outc.newLine();
                    outc.write("end");
                    outc.newLine();
                    outc.flush();
                    // 读取成功/失败信息
                    String msg = inc.readLine();
                    System.out.println(msg);
                    toCopy.close();
                }


            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Bestserver 发送消息错误");
            }
        }
    }
}

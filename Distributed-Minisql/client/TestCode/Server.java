import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CopyOnWriteArrayList;

/**
  *@类名 Chat
  *@描述 TODO Socket服务??(测试??)
  *@版本 1.0
  *@创建?? XuKang
  *@创建时间 2020/9/24 16:17
  **/
  public class Server {

	private static CopyOnWriteArrayList<Channel> all =new CopyOnWriteArrayList<Channel>();

	public static void main(String[] args) throws IOException {
		System.out.println("-----Server-----");
		// 1、指定端?? 使用ServerSocket创建服务??
		ServerSocket server =new ServerSocket(9999);
		// 2、阻塞式等待连接 accept
		while(true) {
				Socket  client =server.accept(); 
				System.out.println("一个客户端建立了连??");
				Channel c =new Channel(client);
				all.add(c); //管理所有的成员
				new Thread(c).start();			
			}		
		}
		//一个客户代表一个Channel
		static class Channel implements Runnable{
			private BufferedReader cin;
    private BufferedWriter cout;
			
			private Socket  client;			
			private boolean isRunning;
			private String name;
			public Channel(Socket  client) {
				this.client = client;
				try {
					cout = new BufferedWriter(new java.io.OutputStreamWriter(client.getOutputStream()));
       	 cin = new BufferedReader(new java.io.InputStreamReader(client.getInputStream()));
        
					
					isRunning =true;
					//获取名称
					this.name =receive();//退出出聊天??
					//欢迎你的到来
					this.send("server:欢迎你的到来");
					sendOthers(this.name+"来了聊天??",true);//暂时固定为私??
				} catch (IOException e) {
					System.out.println("---1------");
					release();					
				}			
			}
			//接收消息
			private String receive() {
				String msg ="";
				try {
					while( (msg = cin.readLine()) != null) {
						if (msg.equals("end")) {
							release();
							break;
						}
						
						
						
							send("<ip>:table:localhost");
						
						System.out.println(msg);
					}
	
					
				} catch (IOException e) {
					System.out.println("---2------");
					release();
				}
				return msg;
			}
			//发送消??
			private void send(String msg) {
				try {
					cout.write(msg);
					cout.newLine();
					cout.write("end");
					cout.newLine();
					cout.flush();
				} catch (IOException e) {
					System.out.println("---3------");
					release();
				}
			}
			/**
			 * @方法?? sendOthers
			 * @描述 TODO 群聊：获取自己的消息，发给其他人，需要设置isSys为false
			 * 		 TODO 私聊: 约定数据格式: @xxx:msg
			 * @参数 msg 发送内??
			 * @返回??
			 * @创建?? XuKang
			 * @创建时间 2020/9/24 16:28
			 */
			private void sendOthers(String msg,boolean isSys) {
				boolean isPrivate = msg.startsWith("@");
				if(isPrivate) { //私聊
					int idx =msg.indexOf(":");
					//获取目标和数??
					String targetName = msg.substring(1,idx);
					msg = msg.substring(idx+1);
					for(Channel other: all) {
						if(other.name.equals(targetName)) {//目标
							other.send(this.name +"悄悄地对您说:"+msg);
							break;
						}
					}
				}else {				
					for(Channel other: all) {
						if(other==this) { //自己
							continue;
						}
						if(!isSys) {
							other.send(this.name +"对所有人??:"+msg);//群聊消息
						}else {
							other.send(msg); //系统消息
						}
					}
				}
			}
			//释放资源
			private void release() {
				this.isRunning = false;
				// CloseUtils.close(dis,dos,client);
				//退??
				all.remove(this);
				sendOthers(this.name+"离开大家??...",true);
			}
			@Override
			public void run() {
				while(isRunning) {
					String msg = receive() ;
					if(!msg.equals("")) {
						//send(msg);
						sendOthers(msg,false);
					}
				}
			}
		}
}


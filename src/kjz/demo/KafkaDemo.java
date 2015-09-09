package kjz.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import kjz.demo.kafka.ConsumerDemo;
import kjz.demo.kafka.ProducerDemo;
import kjz.demo.spark.SparkStreamDemo;

public class KafkaDemo {
	public static void main(String[] args) throws IOException {
		String brokerList =args[1];
		switch(args[0]){
		case "consumer":
			/* String zooKeeper = args[0];
	        String groupId = args[1];
	        String topic = args[2];*/
			//String zooKeeper="172.17.0.4:3000,172.17.0.5:3000,172.17.0.6:3000";
			//String groupId="group0";
			String topic="test";
	        //int threads = Integer.parseInt(args[3]);
			SparkStreamDemo sparkStreamDemo = new SparkStreamDemo(brokerList,topic);
			sparkStreamDemo.runTerminalDemo();
	        //ConsumerDemo example = new ConsumerDemo(zooKeeper, groupId, topic);
	        //example.run(threads);
	        //example.runSingleConsumer();
	 /*
	        try {
	            Thread.sleep(10000);
	        } catch (InterruptedException ie) {
	 
	        }
	 */
	        //example.shutdown();
	        break;
		case "producer":
			ProducerDemo demo=new ProducerDemo(brokerList);
			ServerSocket listener = null;
			try {
				listener = new ServerSocket(9090);
				while (true) {
	                Socket socket = listener.accept();
	                try {
	                	PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
	                	BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	                	String line;
	                    while((line=in.readLine())!= null){
	                    	demo.runDemo("test", "key",line);
	                    }
	                    in.close();
	                } finally {
	                    socket.close();
	                }
	            }
			}finally{
	            listener.close();
				demo.Close_Producer();
			}
		default:
			System.out.println("choose your modle!");
		}
       
    }

}

package kjz.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.configuration.INIConfiguration;

import kjz.demo.kafka.ConsumerDemo;
import kjz.demo.kafka.ProducerDemo;
import kjz.demo.spark.SparkStreamDemo;

public class KafkaDemo {
	public static void main(String[] args) throws IOException {
		switch(args[0]){
		case "consumer":
			String zooKeeper=args[1];
			String groupId="group0";
			String topic="test";
			switch(args[2]){
			case "single":
				ConsumerDemo example = new ConsumerDemo(zooKeeper, groupId, topic+"1");
				example.runSingleConsumer();
				break;
			case "multi":
				int num = Integer.parseInt(args[3]);
				ConsumerDemo demoThread = new ConsumerDemo(zooKeeper, groupId, topic);
				demoThread.run(num,3);
				//demoThread.shutdown(10000000);
				break;
			default:
				System.out.println("Choose your mode:[single], or [multi] [num]");
			}
	        break;
		case "producer":
			String brokerList =args[1];
			String mode=args[2];
			ProducerDemo demo=new ProducerDemo(brokerList);
			switch(mode){
			case "listener":
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
		                    	System.out.println(line);
		                    	demo.runListenerDemo("test", "key",line);
		                    }
		                    in.close();
		                } finally {
		                    socket.close();
		                }
		                break;
		            }
				}finally{
		            listener.close();
					demo.Close_Producer();
					
				}
			case "generater":
				demo.runGeneraterDemo(Integer.parseInt(args[3]));
				break;
			default:
				System.out.println("Choose your mode: [listener], or [generator] [nums]");
			}
			
			break;
		default:
			System.out.println("Choose your role, [consumer] or [producer]?");
		}
       
    }

}

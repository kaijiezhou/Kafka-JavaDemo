package kjz.demo.kafka;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class ProducerDemo {
	private String brokerList;
	private String msg;
	public ProducerDemo(String brokerList,String msg){
		this.brokerList=brokerList;
		this.msg=msg;
	}
	public void runDemo(long events){
		//Random rnd = new Random();
		 
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", "key", msg);
        producer.send(data);
        /*for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "92.168.2." + rnd.nextInt(255); 
               String msg = runtime + ", www.example.com," + ip; 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", ip, msg);
               producer.send(data);
        }*/
        producer.close();
    }
}

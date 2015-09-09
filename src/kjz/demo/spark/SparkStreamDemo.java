package kjz.demo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreamDemo {
	private String sourceHost;
	private int sourcePort;
	private String topics;
	private String brokerList;
	public SparkStreamDemo(String sourceHost, int sourcePort,String brokerList,String topics){
		this.sourceHost=sourceHost;
		this.sourcePort=sourcePort;
		this.brokerList=brokerList;
		this.topics=topics;
	}
	public void runTerminalDemo(){
		SparkConf conf=new SparkConf().setAppName("SparkStreamDemo");
		JavaStreamingContext ctx=new JavaStreamingContext(conf, Durations.seconds(1));
		
	}
}

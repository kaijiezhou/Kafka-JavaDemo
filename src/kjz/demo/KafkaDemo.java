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
		SparkStreamDemo demo = new SparkStreamDemo("192.168.0.105:9092,192.168.0.106:9092,192.168.0.107:9092,192.168.0.108:9092", "topic1,topic2");
		demo.runTerminalDemo();
	}
}

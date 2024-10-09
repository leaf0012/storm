package com.storm.kafka;

import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class StormKafkaTopo {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		// TODO Auto-generated method stub
		BrokerHosts brokerHosts = new ZkHosts("10.10.12.17:2181");
		String topic = "topic1";
		String broker = "/brokers";
		String id = "Topo";
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, broker, id);
		spoutConfig.ignoreZkOffsets = false;

		Config conf = new Config();
		// 配置KafkaBolt生成的topic
		conf.put("topic", "topic2");
		
		// 配置kafkablot需要的producer config数据
		Properties props = new Properties();
		props.put("bootstrap.servers", "10.10.12.17:9092");	// 该地址是集群的子集，用来探测集群。
		props.put("acks", "all");	// 记录完整提交，最慢的但是最大可能的持久化
		props.put("retries", 3);	// 请求失败重试的次数
		props.put("batch.size", 16384);	// batch的大小
		props.put("linger.ms", 1);	// 默认情况即使缓冲区有剩余的空间，也会立即发送请求，设置一段时间用来等待从而将缓冲区填的更多，单位为毫秒，producer发送数据会延迟1ms，可以减少发送到kafka服务器的请求数据
		props.put("buffer.memory", 33554432);	// 提供给生产者缓冲内存总量
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");	// 序列化的方式，
																								// ByteArraySerializer或者StringSerializer
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		KafkaBolt<String, Integer> blot = new KafkaBolt<String, Integer>();
		blot.withProducerProperties(props);
		
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new KafkaSpout(spoutConfig), 1);
		builder.setBolt("bolt", new SenqueceBolt(), 1).shuffleGrouping("spout");
		builder.setBolt("kafkabolt", blot, 1).setNumTasks(1).shuffleGrouping("bolt");

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Topo", conf, builder.createTopology());
			Utils.sleep(100000);
			cluster.killTopology("Topo");
			cluster.shutdown();
		}

	}

}

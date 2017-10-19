package com.applications.streaming.consumers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;


import com.applications.streaming.producers.AvroProducer;import com.twitter.bijection.Injection;import com.twitter.bijection.avro.GenericAvroCodecs;



public class AvroConsumer {

	private static SparkConf sc = null;
	private static JavaSparkContext jsc = null;
	private static JavaStreamingContext jssc = null;
	private static Injection<GenericRecord,byte[]> inject = null;
	
	static {
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(AvroProducer.schema);
		inject = GenericAvroCodecs.apply(schema);
	}

	public static void startAvroConsumer() throws InterruptedException {
		sc = new SparkConf().setAppName("Spark Avro Streaming Consumer")
				.setMaster("local[*]");
		jsc = new JavaSparkContext(sc);
		jssc = new JavaStreamingContext(jsc, new Duration(200));

		Set<String> topics = Collections.singleton("jason");
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		JavaPairInputDStream<String, byte[]> inputDstream = KafkaUtils
				.createDirectStream(jssc, String.class, byte[].class,
						StringDecoder.class, DefaultDecoder.class, kafkaParams,
						topics);
		
		inputDstream.map(message -> inject.invert(message._2).get()).foreachRDD(rdd -> {
				rdd.foreach(record -> {
					System.out.println(record.get("str1"));
					System.out.println(record.get("str2"));
					System.out.println(record.get("int1"));
				});
			});
		
		jssc.start();
		jssc.awaitTermination();
	}

}

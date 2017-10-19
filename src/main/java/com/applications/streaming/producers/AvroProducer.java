package com.applications.streaming.producers;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

public class AvroProducer {
	
	public static final String schema = "{"
    +"\"fields\": ["
    +   " { \"name\": \"str1\", \"type\": \"string\" },"
    +   " { \"name\": \"str2\", \"type\": \"string\" },"
    +   " { \"name\": \"int1\", \"type\": \"int\" }"
    +"],"
    +"\"name\": \"myrecord\","
    +"\"type\": \"record\""
    +"}"; 
	
	public static void startAvroProducer() throws InterruptedException{
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "Kafka Avro Producer");
		
		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(AvroProducer.schema);
		
	    Injection<GenericRecord, byte[]> inject = GenericAvroCodecs.toBinary(schema);
	    
	    KafkaProducer<String,byte[]> producer = new KafkaProducer<String,byte[]>(props);
	    for(int i=0;i<1000;i++){
	    	GenericData.Record record = new GenericData.Record(schema);
	    	record.put("str1", "str1-"+i);
	    	record.put("str2", "str2-"+i);
	    	record.put("int1", i);
	    	
	    	byte[] bytes = inject.apply(record);
	    	
	    	ProducerRecord<String,byte[]> producerRec = new ProducerRecord<String, byte[]>("jason", bytes);
	    	producer.send(producerRec);
	    	Thread.sleep(250);
	    	
	    }
		
	    producer.close();
	}

}

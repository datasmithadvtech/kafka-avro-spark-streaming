package com.applications.streaming.StreamingApplication;

import com.applications.streaming.consumers.AvroConsumer;
import com.applications.streaming.producers.AvroProducer;

public class MainApp {

	public static void main(String[] args) {
		Thread producer = new Thread(new Runnable(){

			@Override
			public void run() {
				try {
					AvroProducer.startAvroProducer();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
		});
		
		Thread consumer = new Thread(new Runnable(){

			@Override
			public void run() {
               try {
				AvroConsumer.startAvroConsumer();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}				
			}
			
		});
		
		producer.start();
		consumer.start();
	}

}

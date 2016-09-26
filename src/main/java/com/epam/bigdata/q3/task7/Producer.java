package com.epam.bigdata.q3.task7;

import com.google.common.io.Resources;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class Producer {
public static final String PARAMS_ERROR = "Usage: producer <file_path>";
	
    public static void main(String[] args) throws IOException {
    	
    	if (args.length < 2) {
			System.err.println(PARAMS_ERROR);
			System.exit(2);
		}
    	
        Path path = new Path(args[1]);
    	BufferedReader br = null;
    	FileSystem fs = null;
    	
        // Set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

    	try{
    		fs = FileSystem.get(new Configuration());
			br = new BufferedReader(new InputStreamReader(fs.open(path)));	
			System.out.println("starts reading file");
			String line = br.readLine();
			while(line != null) {
				producer.send(new ProducerRecord<String, String>("fast-messages", line));
				producer.send(new ProducerRecord<String, String>("summary-markers", line));
				System.out.println("send message:" + line);
				line = br.readLine();
			}
    				
    	} catch (IOException e) {
    		System.out.println("Exception while reading file: " + e.getMessage());
    	}
    	catch (Throwable e) {
          System.out.println(e.getMessage());
      } finally {
    	  System.out.println("FINISH!!!");
          producer.close();
      }
    }
}

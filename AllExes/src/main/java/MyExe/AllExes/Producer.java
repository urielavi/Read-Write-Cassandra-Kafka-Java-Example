package   MyExe.AllExes;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;



public class Producer {
	
	//one message producer
	public void produce(String topic, String key, String value) {
		
		//settings
		Properties properties = new Properties();						
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer <String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        
        kafkaProducer.send(new ProducerRecord<String, String>(topic, key, value));
        kafkaProducer.close();
	}
	
		
    public static void main(String[] args){
     
        	
    	/* multiple producing 
    	
    	Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer" , "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer <String, Integer> kafkaProducer = new KafkaProducer<String, Integer>(properties);
                
        try{ 	
         
        	for(int i = 150; i < 200; i++){
                System.out.println(i);
                kafkaProducer.send(new ProducerRecord("input-message", Integer.toString(i), "test message - " + i ));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }
    */
    }
}
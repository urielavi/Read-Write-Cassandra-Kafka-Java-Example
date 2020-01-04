package MyExe.AllExes;


import static org.junit.Assert.assertEquals;


public class KafkaTest {
	

	@org.junit.Test
	public void kafkaEven(){
		//for producer
		Producer setTest=new Producer();
		String setKey="keyTest";
		String setValue="valueTest";
		setTest.produce("input-message", setKey, setValue);
		
		
		//for consumer
		Consumer getTest = new Consumer();
		getTest.subscribe();
        String getValue;  
		getValue=getTest.readLastMessage("input-message", 0);
        
		
		//even?
		assertEquals(setValue, getValue);
		}
}

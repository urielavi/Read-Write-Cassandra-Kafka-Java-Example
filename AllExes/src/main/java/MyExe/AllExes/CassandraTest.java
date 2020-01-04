package MyExe.AllExes;

import static org.junit.Assert.assertEquals;

public class CassandraTest {
	

	@org.junit.Test
	public void cassandraEven(){
		//for producer

		CassandraCon test=new CassandraCon();
		
		//write
		int amnt=3000;
		test.write("bank", "transfers", 1000, amnt, "mizrachi", "poalim");
		
		
		
		
		//read
		int getAmnt=10;
		getAmnt=test.read("bank", "transfers", 1000);
		     
		
		//even?
		System.out.println(amnt);
		System.out.println(getAmnt);
		
		assertEquals(amnt, getAmnt);
		}
}

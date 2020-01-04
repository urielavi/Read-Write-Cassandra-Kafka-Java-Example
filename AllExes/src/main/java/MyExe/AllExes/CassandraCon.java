 package MyExe.AllExes;

import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ResultSet;

public class CassandraCon {

	public void write(String myCluster, String keySpace, int id, int amount, String reciver, String sender){
		
		Cluster clusterName;
		Session session;
		clusterName=Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
		clusterName.getConfiguration().getSocketOptions().setReadTimeoutMillis(100000);
		session = clusterName.connect(myCluster);
		
		//PreparedStatement preparedStatement = null;
		
		String strQuery ="INSERT INTO "+keySpace+" (id, ammount,receiver, sender) "
				+ "VALUES ("+id+", "+amount+", '"+reciver+"', '"+sender+"')";
		
		session.execute(strQuery);
				

		clusterName.close();
		System.out.println("done");
		
	}
	
	public int read(String myCluster, String keySpace, int id){
		
		Cluster cluster;
		Session session;
		cluster=Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
		cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(100000);
		session = cluster.connect(myCluster);

		String rcvName = null, sndName = null;
		int amnt = 0, trnsId = 0;
		//ResultSet resultSet=session.execute("select * from " +table+" where id="+id);
		ResultSet resultSet=session.execute("select * from transfers");
		// where id=1000
				
		for(Row row:resultSet)   //column loop
		{
			
			trnsId = row.getInt("id");

			if(trnsId==id) {
				amnt = row.getInt("ammount");
				rcvName = row.getString("receiver");
				sndName = row.getString("sender");
					
			}
			
			System.out.println("ID: " + trnsId);
			System.out.println("amount: " + amnt);
			System.out.println("receiver: " + rcvName);
			System.out.println("sender: "+ sndName);		
			
			
		}
		

		cluster.close();
		System.out.println("done");
		return amnt;
	
	}
	
	
	
	public static void main(String[] args) {
		
		
		/*
		Cluster cluster;
		Session session;
		cluster=Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
		cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(100000);
		session = cluster.connect("bank");
		
		//Write
	
		session.execute("INSERT INTO transfers (id, ammount,receiver, sender) VALUES (1, 2000, 'poalim', 'leumi')");
		session.execute("INSERT INTO transfers (id, ammount,receiver, sender) VALUES (2, 8000, 'poalim', 'leumi')");
		session.execute("INSERT INTO transfers (id, ammount,receiver, sender) VALUES (3, 10000, 'poalim', 'leumi')");
		session.execute("INSERT INTO transfers (id, ammount,receiver, sender) VALUES (4, 20000, 'poalim', 'leumi')");
		session.execute("INSERT INTO transfers (id, ammount,receiver, sender) VALUES (5, 25000, 'poalim', 'leumi')");
		session.execute("INSERT INTO transfers (id, ammount,receiver, sender) VALUES (6, 30000, 'poalim', 'leumi')");
		
		
		
		//Read
		
		String rcvName = null, sndName = null;
		int amntId = 0, trnsId = 0;
		ResultSet resultSet=session.execute("select * from transfers");
		for(Row row:resultSet)   //column loop
		{
			trnsId = row.getInt("id");
			amntId = row.getInt("ammount");
			rcvName = row.getString("receiver");
			sndName = row.getString("sender");
			
			System.out.println("ID: " + trnsId);
			System.out.println("amount: " + amntId);
			System.out.println("receiver: " + rcvName);
			System.out.println("sender: "+ sndName);
		
		}
		
		cluster.close();
		System.out.println("done");
		
		
		*/
	}
}
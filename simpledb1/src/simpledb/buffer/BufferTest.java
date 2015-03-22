package simpledb.buffer;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import simpledb.file.Block;
import simpledb.remote.SimpleDriver;
import simpledb.server.SimpleDB;

/**
 * 
 * @author Eyüp İBİŞOĞLU
 *
 */
public class BufferTest
{
	public static void main(String[] args)
	{
//		createTable(); // Tabloyu oluşturmaktadır. person tablosu mevcut değilse etkin hale getirilmelidir.
		
		SimpleDB.initFileLogAndBufferMgr("studentdb");
		BufferMgr bufferMgr = SimpleDB.bufferMgr();
		
		Block block = new Block("person.tbl", 0);
		Block block2 = new Block("person.tbl", 1);
		Block block3 = new Block("person.tbl", 2);
		
		Buffer buffer = bufferMgr.pin( block );
		Buffer buffer2 = bufferMgr.pin(block2);
		Buffer buffer3 = bufferMgr.pin(block3);
		
		bufferMgr.unpin( buffer2 );
		bufferMgr.unpin(buffer);
		
		System.out.println( bufferMgr.toString() ); // Tüm buffer'ları Pinned yada Unpinned şeklinde ekrana yazdırmaktadır.
		System.out.println( "Available Buffer : " +  bufferMgr.available() );
	}
	
	public static void createTable()
	{
		Connection connection = null;
		Driver driver = new SimpleDriver();
		try
		{
			connection = driver.connect("jdbc:simpledb://localhost", null);
			Statement statement = connection.createStatement();
			
//			/** Delete Statement **/
//			statement.executeUpdate("delete from person");
			
			/** Create Statement **/
			statement.executeUpdate( "CREATE TABLE person(id int, name varchar(10ResultSet rs = stmt.executeQuery(qry)" );
			
			/** Insert Statement **/
			String[] people = new String[] {"Eyup", "Ahmet", "Mehmet"};
			int i = 1;
			for (String person : people)
				statement.executeUpdate("insert into person (id, name) values (" + i++ + ", '" + person + "')");
			
			ResultSet resultSet = statement.executeQuery("select id,name from person");
			
			while ( resultSet.next() ) 
			{
				int id = resultSet.getInt("id");
				String name = resultSet.getString("name");
				
				System.out.println(id + "\t" + name);
			}
			
			resultSet.close();
			
			
			
			connection.close();
			
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
}

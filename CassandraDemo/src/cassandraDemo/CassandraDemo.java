package cassandraDemo;

import java.util.List;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ColumnDefinitions.Definition;

public class CassandraDemo {

	public static void main(String[] args) {
		String myServer=args[0]; 
		Cluster cluster = Cluster.builder().addContactPoint(myServer).build();

		Session myKeySpace = cluster.connect("guy");
		String cqlString_ = "SELECT * " + " FROM system.schema_columnfamilies" + " WHERE keyspace_name = 'guy'";

		ResultSet myResults1 = myKeySpace.execute(cqlString_);
		List<Definition> colDefs1 = myResults1.getColumnDefinitions().asList();
		System.out.println("Column count=" + colDefs1.size());
		for (Definition colDef : colDefs1) {
			System.out.println(colDef.getName());
		}
		for (Row row : myResults1.all()) {
			System.out.println(row.getString(0) + " " + row.getString(1));
		}

		String cqlString = "SELECT * FROM friends where name='Guy'";
		ResultSet myResults = myKeySpace.execute(cqlString);
		for (Row row : myResults.all()) {
			System.out.println(row.getString(0) + " " + row.getString(1) + " " + row.getString(2));
		}

		List<Definition> colDefs = myResults.getColumnDefinitions().asList();
		System.out.println("Column count=" + colDefs.size());
		System.out.println("Column Names:"); 
		for (Definition colDef : colDefs) {
			System.out.println(colDef.getName());
		}
		for (Row row : myResults.all()) {
			System.out.println(row.getString(0) + " " + row.getString(1));
		}

	}

}

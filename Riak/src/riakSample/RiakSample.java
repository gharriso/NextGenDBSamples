package riakSample;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.ConflictResolverFactory;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;

public class RiakSample {

	public static void main(String[] args) {
		try {
			String myServer = args[0];
			RiakClient myClient = RiakClient.newClient(myServer);
			// Create the key, value and set the bucket
			String myKey = Long.toString(System.currentTimeMillis());
			String myValue = myKey + ":" + Thread.getAllStackTraces().toString();

			Location myLocation = new Location(new Namespace("MyBucket"), myKey);

			// Put the data (a Java object will be serialzed to a JSON document)

			StoreValue sv = new StoreValue.Builder(myValue).withLocation(myLocation).build();
			StoreValue.Response svResponse = myClient.execute(sv);
			System.out.println("response=" + svResponse);

			// Pull it back out
			FetchValue fv = new FetchValue.Builder(myLocation).build();
			FetchValue.Response fvResp = myClient.execute(fv);
			String myFetchedData = fvResp.getValue(String.class);
			System.out.println("value=" + myFetchedData);

			// Specify sloppy quorum of 2 nodes ("Sloppy" because it gets the 2
			// "healthy" nodes
			// Not the two absolutely available nodes
			myKey = Long.toString(System.currentTimeMillis());
			myLocation = new Location(new Namespace("MyBucket"), myKey);

			sv = new StoreValue.Builder(Thread.getAllStackTraces()).withLocation(myLocation)
					.withOption(StoreValue.Option.W, Quorum.oneQuorum()).build();
			svResponse = myClient.execute(sv);
			System.out.println("response=" + svResponse);

			// We could specify options for quorum writes

			// Pull it back out
			fv = new FetchValue.Builder(myLocation).build();
			fvResp = myClient.execute(fv);
			myFetchedData = fvResp.getValue(String.class);
			System.out.println("value=" + myFetchedData);

			// Setup up a conflict resolver
			ConflictResolverFactory factory = ConflictResolverFactory.getInstance();
			factory.registerConflictResolver(String.class, new MyResolver());

			//
			// To resolve siblings, we normally ahve to read, resolve and write.
			// UpdateValue
			// encapsulates this logc
			//
			
			//Initial value = 1 
			sv = new StoreValue.Builder("1").withLocation(myLocation).build();
			svResponse = myClient.execute(sv);
			System.out.println("response=" + svResponse);
			
			
			for (int dataUpdate=2;dataUpdate<10;dataUpdate++) {
				 
				String newData=Integer.toString( dataUpdate); 
				
				MyRiakUpdater myUpdatedData = new MyRiakUpdater(newData);
				UpdateValue myUpdate = new UpdateValue.Builder(myLocation)
						.withUpdate(myUpdatedData)
				//		.withStoreOption(StoreValue.Option.RETURN_BODY, true)
						.build();

				UpdateValue.Response updateResponse = myClient.execute(myUpdate);
				System.out.println("response=" + updateResponse);
			}
			fv = new FetchValue.Builder(myLocation).build();
			fvResp = myClient.execute(fv);
			myFetchedData = fvResp.getValue(String.class);
			System.out.println("final value=" + myFetchedData);

			//UpdateValue myUpdate = new UpdateValue.Builder(myLocation).withUpdate(myUpdatedData)
			//		.withStoreOption(StoreValue.Option.RETURN_BODY, true).build();

			//UpdateValue.Response updateResponse = myClient.execute(myUpdate);
			//System.out.println("response=" + updateResponse);

			myClient.shutdown();

		} catch (Exception e) {
			System.out.println(e.getStackTrace());
		}
	}

}

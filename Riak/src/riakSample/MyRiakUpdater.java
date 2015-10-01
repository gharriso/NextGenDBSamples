package riakSample;

//
// This method is used by Riak to do all the modifications after resolution 
//

import com.basho.riak.client.api.commands.kv.UpdateValue;

public class MyRiakUpdater extends UpdateValue.Update<String> {

	private final String updatedString;

	public MyRiakUpdater(String updatedString) {

		this.updatedString = updatedString;
	}

	/*
	 * After the data in Riak has been converted to Objects and any siblings
	 * resolved, UpdateValue.Update<T>.apply() is called and it is where you
	 * will do any and all modifications.
	 * 
	 */

	public String apply(String original) {
		return original + "\n" + updatedString;
	}
}

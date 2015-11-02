package riakSample;

import java.util.List;

import com.basho.riak.client.api.cap.ConflictResolver;

// This class resolves conflicts by keeping the first sibling in the list.
// The logic could just as easily merge the strings, use the longest string, etc. 
public class MyResolver implements ConflictResolver<String> {
	public String resolve(List<String> siblings) {
		return(siblings.get(0) ); 
	}
}

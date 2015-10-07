
/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mongoSample;

import static com.mongodb.client.model.Filters.eq;

import org.bson.Document;

import com.mongodb.BasicDBList;

import com.mongodb.BasicDBObject;

import com.mongodb.MongoClient;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;


  
public class MyMongoDemo {
 
	public static void main(final String[] args) {
		String mongoServer = args[0];

		MongoClient mongoClient = new MongoClient(mongoServer);
		MongoDatabase database = mongoClient.getDatabase("NGDBDemo");
		MongoCollection<Document> collection = database.getCollection("test");

		collection.drop();

		Document people = new Document(); // A document for a person
		people.put("Name", "Guy");
		people.put("Email", "guy@gmail.com");

		BasicDBList friendList = new BasicDBList(); // A list for the persons
													// friends

		BasicDBObject friendDoc = new BasicDBObject(); // A document for each
														// friend

		friendDoc.put("Name", "Jo");
		friendDoc.put("Email", "Jo@gmail.com");
		friendList.add(friendDoc);
		friendDoc.clear();
		friendDoc.put("Name", "John");
		friendDoc.put("Email", "john@gmail.com");
		friendList.add(friendDoc);
		people.put("Friends", friendDoc);

		collection.insertOne(people);

		System.out.println('1'); 
		MongoCursor<Document> cursor = collection.find().iterator();
		try {
			while (cursor.hasNext()) {
				System.out.println(cursor.next().toJson());
			}
		} finally {
			cursor.close();
		}
		System.out.println('2'); 

		for (Document cur : collection.find()) {
			System.out.println(cur.toJson());
		}

		System.out.println('3'); 
		// now use a query to get 1 document out
		Document myDoc = collection.find(eq("Name", "Guy")).first();
		System.out.println(myDoc.toJson());

		database = mongoClient.getDatabase("sakila");
		collection = database.getCollection("films");
		for (Document cur : collection.find()) {
			System.out.println(cur.toJson());
		}
		mongoClient.close();
	}
}

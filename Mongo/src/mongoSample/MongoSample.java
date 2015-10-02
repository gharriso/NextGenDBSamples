
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

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Sorts.descending;

/**
 * The QuickTour code example see:
 * https://mongodb.github.io/mongo-java-driver/3.0/getting-started
 */
public class MongoSample {
	/**
	 * Run this main method to see the output of this quick example.
	 *
	 * @param args
	 *            takes an optional single argument for the connection string
	 */
	public static void main(final String[] args) {
		String mongoServer=args[0]; 
		
		MongoClient mongoClient = new MongoClient( mongoServer);
		MongoDatabase database = mongoClient.getDatabase("sakila");
		MongoCollection<Document> collection = database.getCollection("test");

		// drop all the data in it
		collection.drop();

		// make a document and insert it
		Document doc = new Document("name", "MongoDB").append("type", "database").append("count", 1).append("info",
				new Document("x", 203).append("y", 102));

		collection.insertOne(doc);

		// get it (since it's the only one in there since we dropped the rest
		// earlier on)
		Document myDoc = collection.find().first();
		System.out.println(myDoc.toJson());

		// now, lets add lots of little documents to the collection so we can
		// explore queries and cursors
		List<Document> documents = new ArrayList<Document>();
		for (int i = 0; i < 100; i++) {
			documents.add(new Document("i", i));
		}
		collection.insertMany(documents);
		System.out.println("total # of documents after inserting 100 small ones (should be 101) " + collection.count());

		// find first
		myDoc = collection.find().first();
		System.out.println(myDoc); 
		System.out.println(myDoc.toJson());

		// lets get all the documents in the collection and print them out
		MongoCursor<Document> cursor = collection.find().iterator();
		try {
			while (cursor.hasNext()) {
				System.out.println(cursor.next().toJson());
			}
		} finally {
			cursor.close();
		}

		for (Document cur : collection.find()) {
			System.out.println(cur.toJson());
		}

		// now use a query to get 1 document out
		myDoc = collection.find(eq("i", 71)).first();
		System.out.println(myDoc.toJson());

		// now use a range query to get a larger subset
		cursor = collection.find(gt("i", 50)).iterator();

		try {
			while (cursor.hasNext()) {
				System.out.println(cursor.next().toJson());
			}
		} finally {
			cursor.close();
		}

		// range query with multiple constraints
		cursor = collection.find(and(gt("i", 50), lte("i", 100))).iterator();

		try {
			while (cursor.hasNext()) {
				System.out.println(cursor.next().toJson());
			}
		} finally {
			cursor.close();
		}

		// Query Filters
		myDoc = collection.find(eq("i", 71)).first();
		System.out.println(myDoc.toJson());

		// now use a range query to get a larger subset
		Block<Document> printBlock = new Block<Document>() {
			@Override
			public void apply(final Document document) {
				System.out.println(document.toJson());
			}
		};
		collection.find(gt("i", 50)).forEach(printBlock);

		// filter where; 50 < i <= 100
		collection.find(and(gt("i", 50), lte("i", 100))).forEach(printBlock);

		// Sorting
		myDoc = collection.find(exists("i")).sort(descending("i")).first();
		System.out.println(myDoc.toJson());

		// Projection
		myDoc = collection.find().projection(excludeId()).first();
		System.out.println(myDoc.toJson());

		// Update One
		collection.updateOne(eq("i", 10), new Document("$set", new Document("i", 110)));

		// Update Many
		UpdateResult updateResult = collection.updateMany(lt("i", 100), new Document("$inc", new Document("i", 100)));
		System.out.println(updateResult.getModifiedCount());

		// Delete One
		collection.deleteOne(eq("i", 110));

		// Delete Many
		DeleteResult deleteResult = collection.deleteMany(gte("i", 100));
		System.out.println(deleteResult.getDeletedCount());

		collection.drop();

		// ordered bulk writes
		List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();
		writes.add(new InsertOneModel<Document>(new Document("_id", 4)));
		writes.add(new InsertOneModel<Document>(new Document("_id", 5)));
		writes.add(new InsertOneModel<Document>(new Document("_id", 6)));
		writes.add(new UpdateOneModel<Document>(new Document("_id", 1), new Document("$set", new Document("x", 2))));
		writes.add(new DeleteOneModel<Document>(new Document("_id", 2)));
		writes.add(new ReplaceOneModel<Document>(new Document("_id", 3), new Document("_id", 3).append("x", 4)));

		collection.bulkWrite(writes);

		collection.drop();

		collection.bulkWrite(writes, new BulkWriteOptions().ordered(false));
		// collection.find().forEach(printBlock);

		// Clean up
		//database.drop();

		// release resources
		mongoClient.close();
	}
}

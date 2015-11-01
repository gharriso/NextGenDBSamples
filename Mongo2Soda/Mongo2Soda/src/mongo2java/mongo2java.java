package mongo2java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;
import com.mongodb.*;
import java.util.*;

import oracle.jdbc.*;
import oracle.jdbc.pool.*;
import java.sql.*;
import oracle.soda.*;
import oracle.soda.OracleCursor;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDocument;
import oracle.soda.OracleException;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.soda.rdbms.OracleRDBMSMetadataBuilder;

import java.util.Properties;

import javax.json.*;

public class mongo2java {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		if (args.length != 8) {
			System.out.println("Args: MongoHost MongoDB MongoCollection");
			System.exit(0);
		}
		String mongo = args[0];
		String mongoDb = args[1];
		String collectionName = args[2];
		String oraHost = args[3];
		String oraUser = args[4];
		String oraPass = args[5];
		String oraDbName = args[6];
		String oraCollectionName=args[7]; 

		try {
			MongoClient mongoClient = new MongoClient(mongo);
			MongoDatabase mongoDB = mongoClient.getDatabase(mongoDb);
			MongoCollection<Document> mongoCollection = mongoDB.getCollection(collectionName);
			System.out.println("MongoDB connected");

			OracleConnection oc = getOracleConnection(oraUser, oraPass, oraHost, oraDbName);
			OracleRDBMSClient client = new OracleRDBMSClient();
			OracleDatabase oraDb = client.getDatabase(oc);
			System.out.println("Oracle connected");

			Statement os = oc.createStatement();
			try {
				os.execute("DELETE FROM \"" + oraCollectionName + "\"");
			} catch (SQLException x) {
				System.out.println(x.getMessage());
			}
			OracleRDBMSMetadataBuilder b = client.createMetadataBuilder();
			//OracleDocument collectionMetadata = b.keyColumnAssignmentMethod("client").build();
			OracleDocument collMeta =  client.createMetadataBuilder().keyColumnAssignmentMethod("client").build();
			OracleCollection oraCollection = oraDb.admin().createCollection(oraCollectionName, collMeta);

			// Create a new collection with the specified custom metadata

			//OracleCollection oraCollection = oraDb.admin().createCollection(collectionName, collectionMetadata);
			//OracleCollection oraCollection = oraDb.admin().createCollection(collectionName);
			for (Document mongoDoc : mongoCollection.find()) {
				System.out.println(mongoDoc.toJson());

				// Create a JSON document.
				// OracleDocument doc =
				// oraDb.createDocumentFromString(arg0, arg1)

				// Insert the document into a collection.
				String id = mongoDoc.getInteger("_id").toString();
				OracleDocument oraDoc = oraDb.createDocumentFromString(id, mongoDoc.toJson());
				// OracleDocument oraDoc =
				// oraDb.createDocumentFromString(mongoDoc.toJson());
				oraCollection.insert(oraDoc);
			}
		} catch (Exception x) {
			x.printStackTrace();
		}

	}

	static OracleConnection getOracleConnection(String user, String pass, String host, String db) throws SQLException {
		// Create the data source
		OracleDataSource ods = new OracleDataSource();

		// set connection properties
		ods.setDriverType("thin");
		ods.setNetworkProtocol("tcp");
		ods.setDatabaseName(db);
		ods.setServerName(host);
		ods.setPortNumber(1521);
		ods.setUser(user);
		ods.setPassword(pass);

		// open the connection to the database
		OracleConnection ocon = (OracleConnection) ods.getConnection();
		return (ocon);

	}

	public OracleCollection createCollection(OracleConnection conn, String collectionName) throws OracleException {
		OracleRDBMSClient client = new OracleRDBMSClient();
		OracleDatabase database = client.getDatabase(conn);
		OracleCollection collection = database.admin().createCollection(collectionName);
		return collection;
	};

}

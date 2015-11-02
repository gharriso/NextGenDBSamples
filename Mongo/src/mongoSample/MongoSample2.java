package mongoSample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

public class MongoSample2 { /*
						 * Load some MySQL data into MongoDb
						 */

	private static PreparedStatement actorQry;

	private static PreparedStatement paymentQry;
	private static PreparedStatement rentalQry;
	private static PreparedStatement staffQry;
	
	private static BasicDBList getActors(Connection mysqlConn, Integer filmId)
			throws SQLException {
		BasicDBList actorList = new BasicDBList();

		actorQry.setInt(1, filmId);
		ResultSet actorsRs = actorQry.executeQuery();

		while (actorsRs.next()) { // For each actor

			// Create the actor document
			BasicDBObject actorDoc = new BasicDBObject();
			actorDoc.put("actorId", actorsRs.getInt("ACTOR_ID"));
			actorDoc.put("Last name", actorsRs.getString("LAST_NAME"));
			actorDoc.put("First name", actorsRs.getString("FIRST_NAME"));
			// Add actors to the list of actors
			actorList.add(actorDoc);

		}
		return (actorList);
	}

	private static BasicDBList getInventory(Connection mysqlConn,
			Integer storeId) throws SQLException {
		BasicDBList inventoryList = new BasicDBList();

		String inventorySQL = "SELECT inventory_id,film_id,title "
				+ "          FROM inventory JOIN film USING (film_id) WHERE store_id=?";
		PreparedStatement inventoryQry = mysqlConn
				.prepareStatement(inventorySQL);

		inventoryQry.setInt(1, (int) storeId);
		ResultSet inventoryRs = inventoryQry.executeQuery();

		while (inventoryRs.next()) { // For each actor

			// Create the actor document
			BasicDBObject inventoryDoc = new BasicDBObject();
			inventoryDoc.put("inventoryId", inventoryRs.getInt("INVENTORY_ID"));
			inventoryDoc.put("filmId", inventoryRs.getInt("FILM_ID"));
			inventoryDoc.put("Film Title", inventoryRs.getString("TITLE"));
			inventoryList.add(inventoryDoc);

		}
		return (inventoryList);
	}

	private static BasicDBList getPayments(Connection mysqlConn,
			Integer custId, Integer rentalId) throws SQLException {
		BasicDBList paymentList = new BasicDBList();

		paymentQry.setInt(1, (int) rentalId);
		// paymentQry.setInt(2, (int)custId);
		ResultSet paymentRs = paymentQry.executeQuery();

		while (paymentRs.next()) { // For each payment

			// Create the rental document
			BasicDBObject paymentDoc = new BasicDBObject();
			paymentDoc.put("Payment Id", paymentRs.getInt("PAYMENT_ID"));
			paymentDoc.put("Amount", paymentRs.getFloat("AMOUNT"));
			paymentDoc.put("Payment Date", paymentRs.getString("PAYMENT_DATE"));

			paymentList.add(paymentDoc);

		}

		return (paymentList);
	}

	private static BasicDBList getRentals(Connection mysqlConn, Integer custId)
			throws SQLException {
		BasicDBList rentalList = new BasicDBList();

		rentalQry.setInt(1, (int) custId);
		ResultSet rentalRs = rentalQry.executeQuery();

		while (rentalRs.next()) { // For each rental

			// Create the rental document
			BasicDBObject rentalDoc = new BasicDBObject();
			rentalDoc.put("rentalId", rentalRs.getInt("RENTAL_ID"));
			rentalDoc.put("Rental Date", rentalRs.getString("RENTAL_DATE"));
			rentalDoc.put("Return Date", rentalRs.getString("RETURN_DATE"));
			rentalDoc.put("staffId", rentalRs.getInt("STAFF_ID"));
			rentalDoc.put("filmId", rentalRs.getInt("FILM_ID"));
			rentalDoc.put("Film Title", rentalRs.getString("TITLE"));
			BasicDBList paymentList = getPayments(mysqlConn, custId,
					rentalRs.getInt("RENTAL_ID"));
			// put the actor list into the film document
			rentalDoc.put("Payments", paymentList);
			rentalList.add(rentalDoc);

		}
		return (rentalList);
	}

	private static BasicDBList getStaff(Connection mysqlConn, Integer storeId)
			throws SQLException {
		BasicDBList staffList = new BasicDBList();

		staffQry.setInt(1, (int) storeId);
		ResultSet staffRs = staffQry.executeQuery();

		while (staffRs.next()) { // For each actor

			// Create the actor document
			BasicDBObject staffDoc = new BasicDBObject();
			staffDoc.put("staffId", staffRs.getInt("STAFF_ID"));
			staffDoc.put("First Name", staffRs.getString("FIRST_NAME"));
			staffDoc.put("Last Name", staffRs.getString("LAST_NAME"));
			staffDoc.put("Address", staffRs.getString("ADDRESS"));
			staffDoc.put("City", staffRs.getString("CITY"));
			staffDoc.put("Country", staffRs.getString("COUNTRY"));
			staffDoc.put("Phone", staffRs.getString("PHONE"));
			// Add actors to the list of actors
			staffList.add(staffDoc);

		}
		return (staffList);
	}

	private static void initializeSQL(Connection mysqlConn) throws SQLException {
		String actorSQL = "SELECT actor.first_name, actor.last_name , film_actor.actor_id "
				+ "FROM  sakila.film_actor film_actor "
				+ "   INNER JOIN sakila.actor actor "
				+ "     ON (film_actor.actor_id = actor.actor_id) where film_id=?";
		actorQry = mysqlConn.prepareStatement(actorSQL);

		String paymentSQL = "select rental_id,customer_id, payment_id,amount,payment_date "
				+ "from payment where rental_id=? /*and customer_id=? */";
		paymentQry = mysqlConn.prepareStatement(paymentSQL);

		String rentalSQL = "SELECT rental_id,rental_date, return_date,staff_id, film_id, title "
				+ "  FROM rental "
				+ "  JOIN inventory USING (inventory_id) "
				+ "  JOIN store USING (store_id) "
				+ "  JOIN film USING(film_id) where customer_id=?";
		rentalQry = mysqlConn.prepareStatement(rentalSQL);
		String staffSQL = "SELECT staff_id,first_name,last_name, address,city,country,phone "
				+ "          FROM staff JOIN address USING (address_id) "
				+ "          JOIN city USING (city_id) JOIN country USING (country_id) WHERE store_id=?";
		staffQry = mysqlConn.prepareStatement(staffSQL);
	}

	private static void insertCustomers(Connection mysqlConn, DB mongoDb,
			String mongoCollection) throws SQLException {

		String filmSQL = "SELECT customer_id, "
				+ "     first_name, last_name, address,  district, city, country, phone "
				+ "     FROM customer "
				+ "     JOIN address USING (address_id) "
				+ "     JOIN store USING (store_id) "
				+ "     JOIN city USING (city_id) "
				+ "     JOIN country USING (country_id)";
		Statement query = mysqlConn.createStatement();
		ResultSet custRs = query.executeQuery(filmSQL);

		DBCollection custCollection = mongoDb.getCollection(mongoCollection);
		custCollection.drop();
		Integer custCount = 0;

		while (custRs.next()) { // For each film

			// Create the actors document
			BasicDBObject custDoc = new BasicDBObject();
			Integer custId = custRs.getInt("CUSTOMER_ID");
			custDoc.put("_id", custId);
			custDoc.put("First Name", custRs.getString("FIRST_NAME"));
			custDoc.put("Last Name", custRs.getString("LAST_NAME"));
			custDoc.put("Address", custRs.getString("ADDRESS"));
			custDoc.put("District", custRs.getString("DISTRICT"));
			custDoc.put("City", custRs.getString("CITY"));
			custDoc.put("Country", custRs.getString("COUNTRY"));
			custDoc.put("Phone", custRs.getString("PHONE"));

			BasicDBList rentalList = getRentals(mysqlConn, custId);
			// put the actor list into the film document
			custDoc.put("Rentals", rentalList);

			System.out.print(".");
			custCollection.insert(custDoc); // insert the Customer
			custCount++;
		}
		System.out.println("");
		System.out.println(custCount + " customers loaded into "
				+ mongoCollection);
	}

	private static void insertFilms(Connection mysqlConn, DB mongoDb,
			String mongoCollection) throws SQLException {

		String filmSQL = "SELECT f.title, f.description,f.film_id,  f.length,f.rating,"
				+ "              f.special_features,f.rental_duration,f.replacement_cost ,"
				+ "              c.name category_name "
				+ "         FROM film f join film_category fc using (film_id) "
				+ "         join category c using (category_id)";
		Statement query = mysqlConn.createStatement();
		ResultSet fileRs = query.executeQuery(filmSQL);

		DBCollection filmCollection = mongoDb.getCollection(mongoCollection);
		filmCollection.drop();

		Integer filmCount = 0;

		while (fileRs.next()) { // For each film

			// Create the actors document
			BasicDBObject filmDoc = new BasicDBObject();
			Integer filmId = fileRs.getInt("FILM_ID");
			filmDoc.put("_id", filmId);
			filmDoc.put("Title", fileRs.getString("TITLE"));
			filmDoc.put("Description", fileRs.getString("DESCRIPTION"));
			filmDoc.put("Length", fileRs.getString("LENGTH"));
			filmDoc.put("Rating", fileRs.getString("RATING"));
			filmDoc.put("Special Features",
					fileRs.getString("SPECIAL_FEATURES"));
			filmDoc.put("Rental Duration", fileRs.getString("RENTAL_DURATION"));
			filmDoc.put("Replacement Cost",
					fileRs.getString("REPLACEMENT_COST"));
			filmDoc.put("Category", fileRs.getString("CATEGORY_NAME"));

			BasicDBList actorList = getActors(mysqlConn, filmId);
			// put the actor list into the film document
			filmDoc.put("Actors", actorList);

			System.out.print(".");
			filmCollection.insert(filmDoc); // insert the film
			filmCount++;
		}
		System.out.println("");
		System.out.println(filmCount + " films loaded into " + mongoCollection);
	}

	private static void insertStores(Connection mysqlConn, DB mongoDb,
			String mongoCollection) throws SQLException {

		String storeSQL = "SELECT store.store_id,address,city,country,phone,"
				+ "              staff_id as manager_id, first_name as manager_first_name, "
				+ "              last_name as manager_last_name "
				+ "          FROM store JOIN address USING (address_id)  JOIN city USING (city_id) "
				+ "               JOIN country USING (country_id)"
				+ "               JOIN staff ON (manager_staff_id=staff_id)";
		Statement storeQry = mysqlConn.createStatement();
		ResultSet storeRs = storeQry.executeQuery(storeSQL);

		DBCollection storeCollection = mongoDb.getCollection(mongoCollection);
		storeCollection.drop();

		Integer storeCount = 0;
		while (storeRs.next()) {

			// Create the actors document
			BasicDBObject storeDoc = new BasicDBObject();
			Integer storeId = storeRs.getInt("STORE_ID");
			storeDoc.put("_id", storeId);
			storeDoc.put("Address", storeRs.getString("ADDRESS"));
			storeDoc.put("City", storeRs.getString("CITY"));
			storeDoc.put("Country", storeRs.getString("COUNTRY"));
			storeDoc.put("Phone", storeRs.getString("PHONE"));
			storeDoc.put("managerStaffId", storeRs.getString("MANAGER_ID"));
			storeDoc.put("Manager First Name",
					storeRs.getString("MANAGER_FIRST_NAME"));
			storeDoc.put("Manager Last Name",
					storeRs.getString("MANAGER_LAST_NAME"));

			storeDoc.put("Staff", getStaff(mysqlConn, storeId));
			storeDoc.put("Inventory", getInventory(mysqlConn, storeId));

			System.out.print(".");
			storeCollection.insert(storeDoc); // insert the film
			storeCount++;

		}

		System.out.println("");
		System.out.println(storeCount + " stores loaded into "
				+ mongoCollection);
	}

	public static void main(String[] args) {
		try {
			if (args.length != 6) {
				System.err
						.println("Usage: mongoHost mongoPort mongoDb mongoCollection oracleThinConnection "
								+ "oracleUser oraclePass");
				System.exit(1);
			}
			String mongoHost = args[0];
			Integer mongoPort = Integer.parseInt(args[1]);
			String mongoDb = args[2];

			String mysqlThinConnection = args[3]; /* localhost:3566/sakila */
			String mysqlUser = args[4];
			String mysqlPass = args[5];

			String mysqlConnString = "jdbc:mysql://" + mysqlThinConnection
					+ "?user=" + mysqlUser + "&password=" + mysqlPass;

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			Connection myConnection = DriverManager
					.getConnection(mysqlConnString);
			initializeSQL(myConnection);

			MongoClient m = new MongoClient(mongoHost, mongoPort);

			@SuppressWarnings("deprecation")
			DB db = m.getDB(mongoDb);

			insertFilms(myConnection, db, "films");
			insertCustomers(myConnection, db, "customers");
			insertStores(myConnection, db, "stores");
			System.out.println("Done");
			m.close();

		} catch (Exception x) {
			x.printStackTrace();
			System.exit(2);
		}

	}
}

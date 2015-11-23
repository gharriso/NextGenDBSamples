use mongo.sakila;

SELECT Title FROM films WHERE Rating='G' LIMIT 5;
SELECT Actor[2] FROM films WHERE Title='WEST LION';
 
SELECT Actors[2].`First name`, Actors[2].`Last name` 
  FROM films WHERE Title='WEST LION';
SELECT Title, FLATTEN(Actors.`First Name`) 
  FROM films WHERE Rating='G' LIMIT 5;

SELECT Title, FLATTEN(Actors) 
  FROM films WHERE Rating='G' LIMIT 5;

WITH film_actors AS (   
SELECT Title, FLATTEN(Actors) 
  FROM films )
 SELECT Actors.`First name` FROM film_actors LIMIT 5;  
  
 USE hbase;
 
 SELECT * FROM friends;
   
 WITH friend_details AS 
    (select info, FLATTEN(KVGEN(friends)) AS friend_info FROM friends)
  SELECT CONVERT_FROM(friend_details.info.email,'UTF8') AS email,
         CONVERT_FROM(friend_details.friend_info.`value`,'UTF8') AS friend_email 
    FROM friend_details; 
  
 
 
 
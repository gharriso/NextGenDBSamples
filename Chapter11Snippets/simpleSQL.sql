  SELECT title, length
    FROM film
   WHERE rating = 'PG' 
     AND rental_duration=7
ORDER BY length DESC 
   LIMIT 5
   
   
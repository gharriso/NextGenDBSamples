  SELECT country_name , COUNT (cust_id)
    FROM countries JOIN customers USING (country_id)
   WHERE region = 'Asia'
GROUP BY country_name 
  HAVING COUNT (cust_id) > 500



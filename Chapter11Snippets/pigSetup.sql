create table countries as select country_id, country_name, country_region region from sh.countries;
create table customers as select cust_id,cust_first_name first_name, cust_last_name last_name, country_id from sh.customers; 

/* Formatted on 4/10/2015 2:03:36 PM (QP5 v5.269.14213.34769) */
  SELECT country_name , COUNT (cust_id)
    FROM countries JOIN customers USING (country_id)
   WHERE region = 'Asia'
GROUP BY country_name 
  HAVING COUNT (cust_id) > 500
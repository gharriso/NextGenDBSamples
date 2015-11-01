countrys = LOAD 'COUNTRIES'  USING PigStorage (',') 
	AS (country_id,country_name,region);
customers = load 'CUSTOMERS' USING PigStorage (',') 
	AS (cust_id,  first_name, last_name, country_id); 
asianCountrys = FILTER countrys BY region MATCHES 'Asia';
joinedData = JOIN customers BY country_id, asianCountrys BY country_id; 
groupedData = GROUP joinedData BY country_name;
aggregateData = FOREACH groupedData GENERATE group, 
	COUNT(joinedData.customers::cust_id);
moreThan500cust = FILTER aggregateData BY $1 > 500;
orderedData = ORDER moreThan500cust BY $1 DESC;


DUMP orderedData; 


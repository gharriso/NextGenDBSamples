countrys = load 'COUNTRIES'  using PigStorage (',') AS (country_id,country_name,region);
customers = load 'CUSTOMERS' using PigStorage (',') AS (cust_id,  first_name, last_name, country_id); 
asianCountrys = filter countrys by region matches 'Asia';
joinedData = join customers by country_id, asianCountrys by country_id; 
groupedData = group joinedData by country_name;
aggregateData = foreach groupedData generate group, COUNT(joinedData.customers::cust_id);
moreThan500cust = filter aggregateData by $1 > 500;
orderedData = order moreThan500cust by $1 desc;
dump orderedData; 



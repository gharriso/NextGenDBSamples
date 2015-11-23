  SELECT category, count(*) count
    FROM film_cat
GROUP BY category
ORDER BY count(*) DESC
   LIMIT 5
 
 
 create view film_cat as 
 select title,category.name category from film join film_category using(film_id) 
 join category using (category_id)
select category, count(*) count 
  from film_cat
  group by category
  order by count(*) desc 
  limit 5
 
 
 create view film_cat as 
 select title,category.name category from film join film_category using(film_id) 
 join category using (category_id)
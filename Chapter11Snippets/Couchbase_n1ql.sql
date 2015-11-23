CREATE PRIMARY INDEX filmsIdx ON films USING GSI;
CREATE PRIMARY INDEX customerIdx ON customers USING GSI;
CREATE PRIMARY INDEX overduesIdx ON overdues USING GSI;


 SELECT `Title`  FROM films WHERE _id=200;

 SELECT Actors[0].`First name` , Actors[0].`Last name` 
  FROM films where _id=200;

SELECT `Title`  from films 
 WHERE ANY Actor IN films.Actors SATISFIES 
  ( Actor.`First name`="JOE" AND Actor.`Last name`="SWANK" )END; 

SELECT Actors[0:4] FROM films 
 WHERE ANY Actor IN films.Actors SATISFIES 
  ( Actor.`First name`="JOE" AND Actor.`Last name`="SWANK" )END LIMIT 1; 

SELECT f.`Title` ,a.`First name` ,a.`Last name`
  FROM films f 
 UNNEST f.Actors a
 WHERE f._id=200;

SELECT * FROM overdues; 

SELECT  f.`Title` FROM overdues
  JOIN films f ON KEYS overdues.filmId ; 
  
  

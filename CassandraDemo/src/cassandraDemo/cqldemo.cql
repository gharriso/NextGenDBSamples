use guy;

DROP TABLE friends; 

CREATE TABLE friends 
 (name text,
  friend_name text,
  friend_email text,
  PRIMARY KEY (name,friend_name)); 
  
  INSERT INTO friends (name,friend_name,friend_email) VALUES('Guy','Jo','Jo@gmail.com');
  INSERT INTO friends (name,friend_name,friend_email) VALUES('Guy','Chris','Chris@gmail.com');
  INSERT INTO friends (name,friend_name,friend_email) VALUES('Guy','John','John@gmail.com');
  INSERT INTO friends (name,friend_name,friend_email) VALUES('Jo','Guy','Guy@gmail.com');
  INSERT INTO friends (name,friend_name,friend_email) VALUES('Jo','John','John@gmail.com');
  
SELECT * FROM friends where name='Guy';

SELECT * FROM friends where name > 'Guy'  ; 
SELECT * FROM friends where name='Guy' and friend_name > 'Guy';



SELECT * FROM friends ORDER BY name; 
SELECT * FROM friends where name = 'Guy' ORDER BY friend_name; 
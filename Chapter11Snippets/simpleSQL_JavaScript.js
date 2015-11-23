 db.films.find({
    "Rating": "PG","Rental Duration": "7"}
    ,  
    {"Title": 1,"Length": 1})
    .sort({"Length": -1})
    .limit(5)


 
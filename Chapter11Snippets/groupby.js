db.films.aggregate ({ "$project" : { "Category" : 1 }}, 
                    { "$group" : { "_id" : "$Category" ,
                                   "count" : { "$sum" : 1 }}},
                    { "$sort" : { "count" : -1 }} ,
                    { "$limit" : 5 }              
                    )
                    
                    
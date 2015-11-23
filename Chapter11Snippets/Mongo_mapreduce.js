db.films.mapReduce(
  /* Map */ function() {emit (this.Category,1);},
  
  /* Reduce */ function(key,values) {return Array.sum(values)} , 
  
  {    out: "MovieRatings"  }
)

db.MovieRatings.find();

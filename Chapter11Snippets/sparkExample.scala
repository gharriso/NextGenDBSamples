val countries=sc.textFile("COUNTRIES")
val customers=sc.textFile("CUSTOMERS")
  
// Country codes for asian countries   
val countryRegions=countries.map(x=>(x.split(",")(0),x.split(",")(2)))
val AsianCountries=countryRegions.filter(x=> x._2.contains("Asia") )
// Country codes and country names 
val countryNames=countries.map(x=>(x.split(",")(0),x.split(",")(1)))

//Count of customers in each country 
val custByCountry=customers.map(x=>(x.split(",")(3),1))
val custByCountryCount=custByCountry.reduceByKey((x,y)=> x+y)
custByCountryCount.foreach(println)

val AsiaCustCount=AsianCountries.join(custByCountryCount)
val AsiaCustCountryNames=AsiaCustCount.join(countryNames)
AsiaCustCountryNames.foreach(println)

//AsiaCustCount.foreach(println)
//
// 
//AsiaCustCount foreach (x=> println(s"${x._1}"))
//
//AsiaCustCount foreach (x=> println(s"${x._2}")
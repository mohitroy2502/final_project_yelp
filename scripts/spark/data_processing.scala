/Top categories used in business reviews (Categories Business Review)
val categories_business_review= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","categories","review_count").repartition(1)
categories_business_review .write.insertInto("yelp. categories_business_review ")                     



//Top businesses with different rated reviews (i.e. cool, funny, useful) (Business Review Rated)
val business_review_rated= hiveCtx.sql("select date,business_id,useful,funny,cool from yelp.review").repartition(1)
business_review_rated.write.insertInto("yelp.business_review_rated ")                     


//Top restaurants in number of listed categories (Restaurant Categories)
val restaurant_categories= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","categories").repartition(1)
restaurant_categories.write.insertInto("yelp.restaurant_categories ")                     

//Average number of reviews per business star rating (Review Per Stars)
val review_per_star= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","review_count","stars").repartition(1)
review_per_star.write.insertInto("yelp.review_per_star")

//Check the Saturday open and close times for few businesses (Saturday Open Close)
val saturday_open_close= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","hours").repartition(1)
Saturday_open_close.write.insertInto("yelp.Saturday_open_close")

//Top states and cities in total number of reviews(Top State Cities)
 val top_states_cities= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("state","ci
ty","review_count","stars").repartition(1)
top_states_cities.write.insertInto("yelp.states_cities")

//Top businesses with high review counts (> 1000)
val review_per_star= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","review_count","stars").repartition(1)
review_per_star.write.insertInto("yelp.review_per_star")

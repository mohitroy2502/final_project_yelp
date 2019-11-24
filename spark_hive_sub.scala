//Top categories used in business reviews (Categories Business Review)
Scala> val categories_business_review= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","categories","review_count").repartition(1)
Scala> categories_business_review .write.insertInto("yelp. categories_business_review ")                     



//Top businesses with different rated reviews (i.e. cool, funny, useful) (Business Review Rated)
Scala> val business_review_rated= hiveCtx.sql("select date,business_id,useful,funny,cool from yelp.review").repartition(1)
Scala> business_review_rated.write.insertInto("yelp.business_review_rated ")                     


//Top restaurants in number of listed categories (Restaurant Categories)
Scala> val restaurant_categories= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","categories").repartition(1)
Scala> restaurant_categories.write.insertInto("yelp.restaurant_categories ")                     

//Average number of reviews per business star rating (Review Per Stars)
Scala> val review_per_star= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","review_count","stars").repartition(1)
Scala> review_per_star.write.insertInto("yelp.review_per_star")

//Check the Saturday open and close times for few businesses (Saturday Open Close)
Scala> val saturday_open_close= hiveCtx.jsonFile("hdfs://0.0.0.0:19000/rawdata/business/business.json").select("business_id","hours").repartition(1)
Scala> Saturday_open_close.write.insertInto("yelp.Saturday_open_close")


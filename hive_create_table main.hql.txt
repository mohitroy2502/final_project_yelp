//Hive Commands to create table for all the 

--table for tip.json

create external table tip (
	user_id string,
	business_id string,
	likes bigint,
	text string,
	date string
)
stored as parquet
location '/user/cloudera/output/yelp/tips';

--table for review.json

create external table review (
    review_id string,
    user_id string,
    business_id string,
    stars bigint,
    text string,
    date string,
    cool bigint,
    funny bigint,
    useful bigint
)
stored as parquet
location  '/user/cloudera/output/yelp/reviews';

/*table for user.json
to be noted here that friends field has been removed from this table as part of normalization */
create external table user (
    user_id string,
    name string,
    review_count bigint,
    yelping_since string,
    useful bigint,
    funny bigint,
    cool bigint,
    fans bigint,
    elite array<bigint>,
    average_stars double,
    compliment_hot bigint,
    compliment_more bigint,
    compliment_profile bigint,
    compliment_cute bigint,
    compliment_list bigint,
    compliment_note bigint,
    compliment_plain bigint,
    compliment_cool bigint,
    compliment_funny bigint,
    compliment_writer bigint,
    compliment_photos bigint
)
stored as parquet
location  '/user/cloudera/output/yelp/user';



/*table for business.json
to be noted here that friends field has been removed from this table as part of normalization */
CREATE EXTERNAL TABLE business (
	address string,
	attributes string,
	business_id string,
	categories array<string>,
	city string,
	hours string,	
	is_open boolean,
	latitude float,
	longitude float,
	name string,
	postal_code string,
	review_count int,
	stars float,
	state string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS parquet
LOCATION '/user/hive/warehouse/yelp_db.db';

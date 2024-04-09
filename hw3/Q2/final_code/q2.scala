// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")    

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata.csv") // the csv file which you want to work with
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")

// COMMAND ----------

// LOAD THE "taxi_zone_lookup.csv" FILE SIMILARLY AS ABOVE. CAST ANY COLUMN TO APPROPRIATE DATA TYPE IF NECESSARY.
// /FileStore/tables/taxi_zone_lookup.csv
// ENTER THE CODE BELOW
val df_zone = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .load("/FileStore/tables/taxi_zone_lookup.csv") // the csv file which you want to work with

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Some commands that you can use to see your dataframes and results of the operations. You can comment the df.show(5) and uncomment display(df) to see the data differently. You will find these two functions useful in reporting your results.
// display(df)
//df.show(5) // view the first 5 rows of the dataframe

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Filter the data to only keep the rows where "PULocationID" and the "DOLocationID" are different and the "trip_distance" is strictly greater than 2.0 (>2.0).

// VERY VERY IMPORTANT: ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

val df_filter = df.filter($"PULocationID" =!= $"DOLocationID" && $"trip_distance" > 2.0)
// df_filter.show(5)

// COMMAND ----------

// PART 1a: List the top-5 most popular locations for dropoff based on "DOLocationID", sorted in descending order by popularity. If there is a tie, then the one with the lower "DOLocationID" gets listed first

// Output Schema: DOLocationID int, number_of_dropoffs int 

// Hint: Checkout the groupBy(), orderBy() and count() functions.

// ENTER THE CODE BELOW
val top5_DO = df_filter
  .groupBy($"DOLocationID")
  .agg(count("*").alias("number_of_dropoffs"))
  .orderBy(desc("number_of_dropoffs"), asc("DOLocationID"))
  .limit(5)
  .select($"DOLocationID".cast(IntegerType), $"number_of_dropoffs".cast(IntegerType))

// top5_DO.show()

// COMMAND ----------

// PART 1b: List the top-5 most popular locations for pickup based on "PULocationID", sorted in descending order by popularity. If there is a tie, then the one with the lower "PULocationID" gets listed first.
 
// Output Schema: PULocationID int, number_of_pickups int

// Hint: Code is very similar to part 1a above.

// ENTER THE CODE BELOW
val top5_PU = df_filter
  .groupBy($"PULocationID")
  .agg(count("*").alias("number_of_pickups"))
  .orderBy(desc("number_of_pickups"), asc("PULocationID"))
  .limit(5)
  .select($"PULocationID".cast(IntegerType), $"number_of_pickups".cast(IntegerType))

// top5_PU.show()

// COMMAND ----------

// PART 2: List the top-3 locationID’s with the maximum overall activity. Here, overall activity at a LocationID is simply the sum of all pickups and all dropoffs at that LocationID. In case of a tie, the lower LocationID gets listed first.

//Note: If a taxi picked up 3 passengers at once, we count it as 1 pickup and not 3 pickups.

// Output Schema: LocationID int, number_activities int

// Hint: In order to get the result, you may need to perform a join operation between the two dataframes that you created in earlier parts (to come up with the sum of the number of pickups and dropoffs on each location). 

// ENTER THE CODE BELOW
// Calculate the number of pick-ups (considering passengers) and drop-offs at each LocationID
val activity_count = df_filter
  .groupBy($"PULocationID")
  .agg(count("pickup_datetime").alias("pickup_count"))
  .join(
    df_filter.groupBy($"DOLocationID").agg(count("dropoff_datetime").alias("dropoff_count")),
    df_filter("PULocationID") === df_filter("DOLocationID"),
    "full_outer"
  )
  .select(
    coalesce($"PULocationID", $"DOLocationID").alias("LocationID"),
    (coalesce($"pickup_count", lit(0)) + coalesce($"dropoff_count", lit(0))).alias("number_activities")
  )

// Order by the number of activities and LocationID, and select the top 3
val top3_location = activity_count
  .orderBy(desc("number_activities"), asc("LocationID"))
  .limit(3)
  .select($"LocationID".cast(IntegerType), $"number_activities".cast(IntegerType))

// Show the result
// top3_location.show()

// COMMAND ----------

// PART 3: List all the boroughs (of NYC: Manhattan, Brooklyn, Queens, Staten Island, Bronx along with "Unknown" and "EWR") and their total number of activities, in descending order of total number of activities. Here, the total number of activities for a borough (e.g., Queens) is the sum of the overall activities (as defined in part 2) of all the LocationIDs that fall in that borough (Queens). 

// Output Schema: Borough string, total_number_activities int

// Hint: You can use the dataframe obtained from the previous part, and will need to do the join with the 'taxi_zone_lookup' dataframe. Also, checkout the "agg" function applied to a grouped dataframe.

// ENTER THE CODE BELOW
// Join the previous DataFrame with df_zone to get borough information
val activity_borough = activity_count
  .join(df_zone, activity_count("LocationID") === df_zone("LocationID"), "left")
  .select(
    df_zone("Borough"),
    activity_count("number_activities")
  )
  .groupBy("Borough")
  .agg(sum("number_activities").alias("total_number_activities"))
  .orderBy(desc("total_number_activities"))

// Show the result 
//activity_borough.show()

// COMMAND ----------

// PART 4: List the top 2 days of week with the largest number of daily average pickups, along with the average number of pickups on each of the 2 days in descending order (no rounding off required). Here, the average pickup is calculated by taking an average of the number of pick-ups on different dates falling on the same day of the week. For example, 02/01/2021, 02/08/2021 and 02/15/2021 are all Mondays, so the average pick-ups for these is the sum of the pickups on each date divided by 3.

//Note: The day of week is a string of the day’s full spelling, e.g., "Monday" instead of the		number 1 or "Mon". Also, the pickup_datetime is in the format: yyyy-mm-dd.

// Output Schema: day_of_week string, avg_count float

// Hint: You may need to group by the "date" (without time stamp - time in the day) first. Checkout "to_date" function.

// ENTER THE CODE BELOW
// Extract the day of the week from the "pickup_datetime" column
val pickupDayOfWeek = df_filter.withColumn("day_of_week", date_format($"pickup_datetime", "EEEE"))
                       
// Calculate the total pick-ups for each day of the week
val totalPickupsByDay = pickupDayOfWeek
  .groupBy("day_of_week")
  .agg(count("*").alias("total_pickups"))

// Calculate the total number of days for each day of the week
val totalDaysByDay = pickupDayOfWeek
  .withColumn("pickup_date", to_date($"pickup_datetime"))
  .groupBy("day_of_week")
  .agg(countDistinct("pickup_date").alias("total_days"))

// Join the two DataFrames to calculate the average pick-ups per day of the week
val averagePickupsByDay = totalPickupsByDay
  .join(totalDaysByDay, "day_of_week")
  .withColumn("avg_count", round($"total_pickups" / $"total_days", 2))

// Order the result in descending order by average_pickups and select the top 2 days
val top2Days = averagePickupsByDay
  .orderBy(desc("avg_count"))
  .limit(2)
  .select("day_of_week", "avg_count")

// Show the result
// top2Days.show()

// COMMAND ----------

// PART 5: For each hour of a day (0 to 23, 0 being midnight) - in the order from 0 to 23(inclusively), find the zone in the Brooklyn borough with the LARGEST number of total pickups. 

//Note: All dates for each hour should be included.

// Output Schema: hour_of_day int, zone string, max_count int

// Hint: You may need to use "Window" over hour of day, along with "group by" to find the MAXIMUM count of pickups

// ENTER THE CODE BELOW
// Extract the hour from "pickup_datetime"
val pickupsByHour = pickupDayOfWeek
  .withColumn("hour", hour($"pickup_datetime"))

// Filter the data to include only records in Brooklyn
val brooklynPickups = pickupsByHour
  .join(df_zone, pickupsByHour("PULocationID") === df_zone("LocationID"))
  .filter($"Borough" === "Brooklyn")

// Group by hour, Zone, and calculate the total number of pick-ups in each zone
val zonePickupCounts = brooklynPickups
  .groupBy("hour", "Zone")
  .agg(count("*").alias("pickup_count"))

// Find the zone with the largest number of pick-ups for each hour
val windowSpec = Window.partitionBy("hour").orderBy(desc("pickup_count"))
val topZoneByHour = zonePickupCounts
  .withColumn("rank", rank().over(windowSpec))
  .filter($"rank" === 1)
  .select("hour", "Zone", "pickup_count")

// Show the result
// topZoneByHour.show(24)
// display(topZoneByHour)  

// COMMAND ----------

// PART 6 - Find which 3 different days in the month of January, in Manhattan, saw the largest positive percentage increase in pick-ups compared to the previous day, in the order from largest percentage increase to smallest percentage increase 

// Note: All years need to be aggregated to calculate the pickups for a specific day of January. The change from Dec 31 to Jan 1 can be excluded.

// Output Schema: day int, percent_change float


// Hint: You might need to use lag function, over a window ordered by day of month.

// ENTER THE CODE BELOW


// Join pickupDayOfWeek with df_zone to obtain "Borough" and "Zone" information
val joinedData = pickupDayOfWeek
  .join(df_zone, pickupDayOfWeek("PULocationID") === df_zone("LocationID"))

// Filter data for Manhattan and January
val janManhattanPickups = joinedData
  .filter($"Borough" === "Manhattan")
  .filter(month($"pickup_datetime") === 1)
  .groupBy(date_format($"pickup_datetime", "dd").alias("day"))
  .agg(count("*").alias("pickup_count"))

// Calculate the percentage increase compared to the previous day
val windowSpec = Window.orderBy("day")
val pickupWithPrevDay = janManhattanPickups
  .withColumn("prev_day_pickup", lag("pickup_count", 1).over(windowSpec))
  .withColumn("percent_change",
    when(isnull($"prev_day_pickup"), 0)
    .otherwise(round(((($"pickup_count" - $"prev_day_pickup") / $"prev_day_pickup") * 100),2))
  )
  .filter($"day" =!= "01")
  .orderBy(desc("percent_change"))

// Select the top 3 days with the largest percentage increases
val top3Days = pickupWithPrevDay
  .limit(3)
  .select("day", "percent_change")

// Show the result
// top3Days.show()

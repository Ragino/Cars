import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc,desc, min, max, from_json, split
from pyspark.sql.types import MapType, StringType
import pyspark.sql.functions as F
import pandas as pd

spark = SparkSession.builder.appName("sw").getOrCreate()

cars = spark.read.json("C:/Users/ragul/projects/sw/supplier_car.json")

car = cars.withColumnRenamed("Attribute Values", "Attribute_values")\
     .withColumnRenamed("Attribute Names","Attribute_names")
car.select("Attribute_values").filter("Attribute_names == 'BodyTypeText'").groupby("Attribute_values").count().show(truncate=False)

df = cars.withColumn("CarType", split(cars['TypeName'], ' ')) \
     .select(F.expr('CarType[2]').alias("Car"), "Attribute Values").filter("Car == 'Coupé' ")
# cars.printSchema()

car.createOrReplaceTempView("data")

d1 = spark.sql(""" SELECT DISTINCT ID, CASE WHEN TypeNameFull like '%Coup%' or (Attribute_names = 'BodyTypeText' and Attribute_values like '%Coup%')
THEN 'Coupé'
WHEN TypeNameFull like '%Cabriolet%' or (Attribute_names = 'BodyTypeText' and Attribute_values like '%Cabriolet%')
THEN 'Convertible / Roadster'
WHEN TypeNameFull like '%Limousin%' or (Attribute_names = 'BodyTypeText' and Attribute_values like '%Limousine%')
THEN 'Saloon'
WHEN Attribute_names = 'BodyTypeText' and Attribute_values like '%Kleinwagen%'
THEN 'Single Seater'
WHEN Attribute_names = 'BodyTypeText' and Attribute_values like '%SUV%' or 'Attribute_values' like '%Geländewagen%'
THEN 'SUV'
WHEN  TypeNameFull like '%Targa%' or TypeNameFull like '%Rennwagen%'
THEN 'Targa'
WHEN Attribute_values = 'Kombi'
THEN 'Station Wagon'
else 'Other' END AS Car_type,
CASE WHEN TypeNameFull like '%Green%'
THEN 'Green'
WHEN TypeNameFull like '%BLACK%' or Attribute_values like '%schwarz%'
THEN 'Black'
WHEN TypeNameFull like '%Red%' or Attribute_values like '%rot%'
THEN 'Red'
WHEN TypeNameFull like '%Blue%' or Attribute_values like '%blau%'
THEN 'Blue'
WHEN TypeNameFull like '%Silver%' or Attribute_values like '%Silber%'
Then 'Silver'
WHEN TypeNameFull like '%gold%' or Attribute_values like '%gold%'
Then 'Gold'
WHEN TypeNameFull like '%yellow%' or Attribute_values like '%gelb%'
THEN 'Yellow'
WHEN TypeNameFull like '%gray%' or Attribute_values like '%grau%'
THEN 'Gray'
WHEN TypeNameFull like '%white%' or Attribute_values like '%weis%'
THEN 'White'
WHEN TypeNameFull like '%Brown%' or Attribute_values like '%braun%'
THEN 'Brown'
WHEN TypeNameFull like '%orange%' or Attribute_values like '%orange%'
THEN 'Orange'
WHEN TypeNameFull like '%beige%' or Attribute_values like '%Beige%'
THEN 'Beige'
ELSE 'Other' END AS Color,
CASE WHEN Attribute_names = 'Condition' AND Attribute_values like '%Neu%'
THEN 'New'
WHEN Attribute_names = 'Condition' AND Attribute_values like '%Occasion%'
THEN 'Used'
WHEN Attribute_names = 'Condition' AND Attribute_values like '%Vorführmodell%'
THEN 'Original condition'
WHEN Attribute_names = 'Condition' AND Attribute_values like '%Oldtimer%'
THEN 'Restored'
ELSE NULL end as Condition,
CASE WHEN Attribute_names like '%Drive%' AND Attribute_values like '%Hinterradantrieb%'
THEN 'RWD'
WHEN Attribute_names like '%Drive%' AND Attribute_values like '%Vorderradantrieb%'
THEN 'LWD'
ELSE NULL END AS Drive,
CASE WHEN Attribute_names = 'City'
THEN Attribute_values
ELSE NULL END AS City,
MakeText as make,
CASE WHEN Attribute_names = 'FirstRegYear' 
THEN Attribute_values 
ELSE null END as Manufactory_year,
ModelText as Model,
CASE WHEN Attribute_names = 'FirstRegMonth' 
THEN Attribute_values 
ELSE null END as Manufactory_month,
ModelTypeText as Model_variant,
'car' as type,
'CH' AS Country,
'CHF' as Currency,
CASE WHEN Attribute_names = 'Km'
THEN Attribute_values
ELSE NULL END AS mileage,
CASE WHEN Attribute_names = 'ConsumptionTotalText'
THEN 'l_km_consumption'
ELSE NULL END AS fuel_consumption_unit
from data
""")

# d1.select("Car_type").groupby("").count().show(truncate=False)

Car_type = d1.withColumnRenamed("ID", "car_ID")\
     .select("car_ID", "Car_type", "Color", "type", "Country", "Currency")
condition = d1.withColumnRenamed("ID", "condition_ID")\
     .select("condition_ID", "Condition").filter("Condition is not NULL")
city = d1.withColumnRenamed("ID", "city_ID")\
     .select("city_ID", "City").filter("City is not NULL")
make = d1.withColumnRenamed("ID", "make_ID")\
     .select("make_ID", "make").filter("make is not null")
model = d1.withColumnRenamed("ID", "model_ID")\
     .select("model_ID", "model").filter("model is not NULL")
man_year = d1.withColumnRenamed("ID", "man_y_ID")\
     .select("man_y_ID", "Manufactory_year").filter("Manufactory_year is not NULL")
man_month = d1.withColumnRenamed("ID", "man_m_ID")\
     .select("man_m_ID", "Manufactory_month").filter("Manufactory_month is not NULL")
model_v = d1.withColumnRenamed("ID", "model_v_ID")\
     .select("model_v_ID", "Model_variant").filter("model_variant is not null")
mileage = d1.withColumnRenamed("ID", "mileage_ID")\
     .select("mileage_ID", "mileage").filter("mileage is not null")
fuel_c_unit = d1.withColumnRenamed("ID", "fuel_c_ID")\
     .select("fuel_c_ID", "fuel_consumption_unit").filter("fuel_consumption_unit is not null")


new_c = Car_type.join(condition, Car_type.car_ID == condition.condition_ID, "left_outer")\
     .join(city, Car_type.car_ID == city.city_ID, "left_outer")\
     .join(make, Car_type.car_ID == make.make_ID, "left_outer")\
     .join(model, Car_type.car_ID == model.model_ID, "left_outer")\
.join(man_year, Car_type.car_ID == man_year.man_y_ID, "left_outer")\
.join(man_month, Car_type.car_ID == man_month.man_m_ID, "left_outer")\
.join(model_v, Car_type.car_ID == model_v.model_v_ID, "left_outer")\
.join(mileage, Car_type.car_ID == mileage.mileage_ID, "left_outer")\
.join(fuel_c_unit, Car_type.car_ID == fuel_c_unit.fuel_c_ID, "left_outer")\
     .select("car_ID", "Car_type","Color", "Currency", "City", "Country", "make", "model", "mileage", "model_variant", "Manufactory_year" ,\
             "Manufactory_month", "fuel_consumption_unit", "type").distinct()

car_pd = new_c.toPandas()
car_pd.to_excel("target_data.xlsx")


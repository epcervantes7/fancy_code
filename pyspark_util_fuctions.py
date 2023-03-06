from pyspark.sql.functions import rand, col
from pyspark.sql.types import StructType, StructField, IntegerType

# Define the schema for the dataframe
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("col1", IntegerType(), True),
    StructField("col2", IntegerType(), True),
    StructField("col3", IntegerType(), True),
    StructField("col4", IntegerType(), True)
])

# Create the dataframe with random integer values between 0 and 3
df = spark.range(10).withColumn("col1", (rand() * 4).cast("int")) \
    .withColumn("col2", (rand() * 4).cast("int")) \
    .withColumn("col3", (rand() * 4).cast("int")) \
    .withColumn("col4", (rand() * 4).cast("int")) \
    .select(col("id").alias("id"), col("col1"), col("col2"), col("col3"), col("col4"))


from pyspark.sql.functions import when, array, lit

# Create a list of column names
col_names = ["col1", "col2", "col3", "col4"]

# Use a list comprehension to create a new column for each column name
for name in col_names:
    df = df.withColumn(name + "_flag", when(col(name) == 3, name).otherwise(None))

# Use the array() function to create an array of column flag columns
col_flag_array = array([col(name + "_flag") for name in col_names])
df = df.withColumn("value_is_3_in", coalesce(col_flag_array))
# df = df.withColumn("riesgos",concat_ws(",",col("value_is_3_in")))

from pyspark.sql.functions import udf

# Define the mapping function that will be applied to each element of the 'value_is_3_in' column
# my_dict = {'col1': 'fruit', 'col2': 'fruit', 'col3': 'vegetable', 'col4': 'vegetable'}
map_func = udf(lambda x: [variable_indicador.get(col_name, None) for col_name in x], ArrayType(StringType()))

# Apply the mapping UDF to the DataFrame
df = df.withColumn("value_is_3_in_mapped", map_func("value_is_3_in"))
df = df.withColumn("result",concat_ws(",",col("value_is_3_in_mapped")))

df.show()

# Load trainsched.txt
df = spark.read.csv("trainsched.txt", header=True)
# Create temporary table called table1
df.createOrReplaceTempView("table1")

# Inspect the columns in the table df
spark.sql("Describe schedule").show()

# Add col running_total that sums diff_min col in each group
query = """
SELECT train_id, station, time, diff_min,
sum(diff_min) OVER (PARTITION BY train_id ORDER BY time) AS running_total
FROM schedule
"""
# Run the query and display the result
spark.sql(query).show()


query = """
SELECT 
ROW_NUMBER() OVER (ORDER BY time) AS row,
train_id, 
station, 
time, 
LEAD(time,1) OVER (ORDER BY time) AS time_next 
FROM schedule
"""
spark.sql(query).show()
# Give the number of the bad row as an integer
bad_row = 7
# Provide the missing clause, SQL keywords in upper case
clause = 'PARTITION BY train_id'


# Give the identical result in each command
spark.sql('SELECT train_id, MIN(time) AS start FROM schedule GROUP BY train_id').show()
df.groupBy('train_id').agg({'time':'min'}).withColumnRenamed('MIN(time)', 'start').show()
# Print the second column of the result
spark.sql('SELECT train_id, MIN(time), MAX(time) FROM schedule GROUP BY train_id').show()
result = df.groupBy('train_id').agg({'time':'min', 'time':'max'})
result.show()
print(result.columns[1])

#to do:
from pyspark.sql.functions import min, max, col
expr = [min(col("time")).alias('start'), max(col("time")).alias('end')]
dot_df = df.groupBy("train_id").agg(*expr)
dot_df.show()
+--------+-----+-----+
|train_id|start|  end|
+--------+-----+-----+
|     217|6:06a|6:59a|
|     324|7:59a|9:05a|
+--------+-----+-----+

#solution:
# Write a SQL query giving a result identical to dot_df
query = "SELECT train_id, min(time) as start, max(time) as end FROM schedule group by train_id"
sql_df = spark.sql(query)
sql_df.show()
#solution end::

#to do:
df = spark.sql("""
SELECT *, 
LEAD(time,1) OVER(PARTITION BY train_id ORDER BY time) AS time_next 
FROM schedule
""")
#solution:
# Obtain the identical result using dot notation
dot_df = df.withColumn('time_next', lead('time', 1)
        .over(Window.partitionBy('train_id')
        .orderBy('time')))
#solution end::


# Load the dataframe
df = spark.read.load('sherlock_sentences.parquet')
# Filter and show the first 5 rows
df.where('id > 70').show(5, truncate=False)


# Split the clause column into a column called words
split_df = clauses_df.select(split('clause', ' ').alias('words'))
split_df.show(5, truncate=False)
# Explode the words column into a column called word
exploded_df = split_df.select(explode('words').alias('word'))
exploded_df.show(10)
# Count the resulting number of rows in exploded_df
print("\nNumber of rows: ", exploded_df.count())


# Word for each row, previous two and subsequent two words
query = """
SELECT
part,
LAG(word, 2) OVER(PARTITION BY part ORDER BY id) AS w1,
LAG(word, 1) OVER(PARTITION BY part ORDER BY id) AS w2,
word AS w3,
LEAD(word, 1) OVER(PARTITION BY part ORDER BY id) AS w4,
LEAD(word, 2) OVER(PARTITION BY part ORDER BY id) AS w5
FROM text
"""
spark.sql(query).where("part = 12").show(10)


# Repartition text_df into 12 partitions on 'chapter' column
repart_df = text_df.repartition( 12,'chapter')
# Prove that repart_df has 12 partitions
repart_df.rdd.getNumPartitions()


#excercise: most common phrases
# Find the top 10 sequences of five words
query = """
SELECT w1, w2, w3, w4, w5, COUNT(*) AS count FROM (
   SELECT word AS w1,
   LEAD(word,1) OVER(Partition by part order by part) AS w2,
   LEAD(word,2) OVER(Partition by part order by part) AS w3,
   LEAD(word,3) OVER(Partition by part order by part) AS w4,
   LEAD(word,4) OVER(Partition by part order by part) AS w5
   FROM text
)
GROUP BY w1, w2, w3, w4, w5
ORDER BY count DESC
LIMIT 10
"""
df = spark.sql(query)
df.show()

#result::/*
    +-----+---------+------+-------+------+-----+
    |   w1|       w2|    w3|     w4|    w5|count|
    +-----+---------+------+-------+------+-----+
    |   in|      the|  case|     of|   the|    4|
    |    i|     have|    no|  doubt|  that|    3|
    | what|       do|   you|   make|    of|    3|
    |  the|   church|    of|     st|monica|    3|
    |  the|      man|   who|entered|   was|    3|
    |dying|reference|    to|      a|   rat|    3|
    |    i|       am|afraid|   that|     i|    3|
    |    i|    think|  that|     it|    is|    3|
    |   in|      his| chair|   with|   his|    3|
    |    i|     rang|   the|   bell|   and|    3|
    +-----+---------+------+-------+------+-----+






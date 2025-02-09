# Unpersists df1 and df2 and initializes a timer
prep(df1, df2)
# Cache df1
df1.cache()
# Run actions on both dataframes
run(df1, "df1_1st")
run(df1, "df1_2nd")
run(df2, "df2_1st")
run(df2, "df2_2nd", elapsed=True)
# Prove df1 is cached
print(df1.is_cached)


# Unpersist df1 and df2 and initializes a timer
prep(df1, df2)
# Persist df2 using memory and disk storage level
df2.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
# Run actions both dataframes
run(df1, "df1_1st")
run(df1, "df1_2nd")
run(df2, "df2_1st")
run(df2, "df2_2nd", elapsed=True)



# List the tables
print("Tables:\n", spark.catalog.listTables())
# Cache table1 and Confirm that it is cached
spark.catalog.cacheTable('table1')
print("table1 is cached: ", spark.catalog.isCached('table1'))
# Uncache table1 and confirm that it is uncached
spark.catalog.uncacheTable('table1')
print("table1 is cached: ", spark.catalog.isCached('table1'))



# Run explain on text_df
text_df.explain()
# Run explain on "SELECT COUNT(*) AS count FROM table1"
spark.sql("SELECT COUNT(*) AS count FROM table1").explain()
# Run explain on "SELECT COUNT(DISTINCT word) AS words FROM table1"
spark.sql("SELECT COUNT(DISTINCT word) AS words FROM table1").explain()
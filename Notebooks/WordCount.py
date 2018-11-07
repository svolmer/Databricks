# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.stagstor.blob.core.windows.net",
  "aMoXv4hNSfY5xlwhUsMgn8MYNKomVYF+x1Jq98tFCWzNgGbAbt00/wNr3H3am1GH/qqE4CovyaAon8OefLiXnw==")

bible_df = spark.read.text("wasbs://sources@stagstor.blob.core.windows.net/Martin_Luther_Uebersetzung_1912.txt")

display(bible_df)

# COMMAND ----------

text_df = bible_df.select(regexp_extract('value', r'^(?<book>\w+) (?<chapter>\d+):(?<verse>\d+) (?<text>.*)$', 4).alias('rows'))

display(text_df)

# COMMAND ----------

def clean(column):
    return trim(lower(regexp_replace(column, '([^\s\w_]|_)+', ''))).alias('rows')
  
cleaned_df = text_df.select(clean('rows'))

display(cleaned_df)

# COMMAND ----------

words_df = cleaned_df.select(split('rows', '\s+').alias('array')) \
                     .select(explode('array').alias('words'))

display(words_df)

# COMMAND ----------

count_df = words_df.groupBy('words').count().orderBy('count', ascending=0)

display(count_df)
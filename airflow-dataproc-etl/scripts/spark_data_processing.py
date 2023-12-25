from pyspark.sql.session import SparkSession
from pyspark.sql.functions import sum,col
from argparse import ArgumentParser


def process_data(output_hive_database:str, output_hive_table: str=None):
    print("Creating Spark Session with Hive Support ...")
    spark = SparkSession.builder.appName("project-1-job").enableHiveSupport().getOrCreate()
    print("Successfully created Spark Session with Hive Support ...")
    
    bucket = "project-airflow"
    orders_path = f"gs://{bucket}/input_data/orders/"
    books_path = f"gs://{bucket}/input_data/books/"

    print("Reading Orders data ...")
    orders = spark.read.csv(orders_path,header=True,inferSchema=True)

    print("Reading Books data ...")
    books = spark.read.format("csv").option("header","true").option("inferSchema","true").load(books_path)

    print("Successfully Read the data. Applying transformations ...")
    books_sold = books.join(orders, "book_id","left").groupBy(col("name").alias("Book")).agg(sum(col("quantity")).alias("Sold")).fillna(0).orderBy(col("Sold").desc())
    print("Successfully completed Data Transformation operations ...")

    # Hive Query Operations
    print(f"Selecting database {output_hive_database}")
    spark.sql(f"USE {output_hive_database};")
    print(f"Using database {output_hive_database} ...")

    print("Writing output in Hive table ...")
    books_sold.write.mode("append").saveAsTable(output_hive_table)
    print("Successfully written data to Hive table: ",output_hive_table)

    print(books_sold.show())

    print("Closing the Spark Session ...")
    spark.stop()
    print("!!! Closed the Spark Session !!!")


if __name__=="__main__":
    parser = ArgumentParser()
    parser.add_argument("--output_hive_database", type=str,required=True, help="Hive database to store the output dataframe")
    parser.add_argument("--output_hive_table", type=str, required=True, help="Hive table to store the output dataframe")
    args = parser.parse_args()
    output_hive_database = args.output_hive_database
    output_hive_table = args.output_hive_table
    print(output_hive_database,output_hive_table)
    
    process_data(output_hive_database,output_hive_table)





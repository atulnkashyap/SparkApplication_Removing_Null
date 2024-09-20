from pyspark.sql.functions import *


def get_correct_date(spark):
    try:
        print("We validate.py")
        output = spark.sql(""""select current_date""")
        print("Printing current data" + str(output.collect()))

    except Exception as e:
        print("Error occurred in getting current_date", str(e))
        raise
    else:
        print("Validation done, going forward")

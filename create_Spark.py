from pyspark.sql import SparkSession

try:
    def get_spark_session(appName, envn):

        if envn == 'DEV':
            master = 'local'
        else:
            master = 'yarn'

        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

        return spark

except Exception as e:
    print(str(e))
import envn as env
from create_Spark import get_spark_session
from validate import get_correct_date
from ingest import load_file, display_df, df_count
from cleanEtlProcess import remove_null
import os
from write import write_df


def main():
    global file_format, file_dir, header, inferschema
    try:
        print("Hi everybody we are in driver")
        print(env.header)
        print(env.envn)
        print("Calling Spark Object")
        spark = get_spark_session(env.appName, env.header)
        print("Validating Spark Object...........")
        get_correct_date(spark)
        for files in os.listdir(env.src_path):
            print("Src_files ::" + files)
            file_dir = env.src_path + '\\' + files
            if files.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferschema = 'NA'
            elif files.endswith('.csv'):
                file_format = 'csv'
                header = env.header
                inferschema = env.inferschema
        print("reading files".format(file_format))

        df_fact = load_file(spark=spark, file_path=file_dir, header=header, inferschema=inferschema,
                            file_format=file_format)

        print("displaying files")
        display_df(df_fact, 'df_fact')

        print("Df count for validating df")
        df_count(df_fact, 'df_name')

        print("Trying to process the data")
        data_process = remove_null(df_fact)

        print("getting processed data")
        display_df(data_process, 'data_process')

        print("Writing Dataframe")
        write_df(data_process)

    except Exception as e:
        print("An error occurred while calling main() please check the trace ===" + str(e))
        raise
    else:
        print("process main function")


if __name__ == '__main__':
    main()
    print("Application Done")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

def load_file(spark, file_format, header, inferschema, file_path):
    global df
    try:
        if file_format == 'csv':
            df = spark. \
                read. \
                format(file_format). \
                option(header=header). \
                option(inferschema=inferschema). \
                load(file_path)
        elif file_format == 'parquet':
            df = spark. \
                read. \
                format(file_format). \
                load(file_path)

    except Exception as e:
        print((str(e)))
        raise
    else:
        print("We have Successfully created the Dataframes")

    return df


def display_df(df, dfName):
    df_show = df.show()

    return df_show


def df_count(df, dfName):
    try:
        print("Count of records".format(dfName))

        df_c = df.count()
    except Exception as exp:
        raise
    else:
        print("Number of records".format(df_c))
    return df_c

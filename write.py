import envn as env


def write_df(df):
    try:
        print("Writing File")
        output_df = df.write.csv(env.output_path)

    except Exception as exp:
        print("While Writing encounter error" + str(exp))
        raise
    else:
        print("File writing is been successfully completed")


def remove_null(df):
    try:

        filled_na = df.fillna("5", subset=["product_id", "transaction_id"])

        print("Count Before Drop Duplicates:", filled_na.count())

        drop_duplicates = filled_na.dropDuplicates(["Name", "product_id", "transaction_id"])

        print("Count After Drop Duplicates:", drop_duplicates.count())

        return drop_duplicates

    except Exception as e:
        print(str(e))
        raise
    finally:
        print("We are moving forward in cleanEtlProcess")

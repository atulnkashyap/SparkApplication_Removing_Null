import os
import pandas as pd

print("We are in Envn file")
os.environ['envn'] = 'DEV'
os.environ['header'] = 'True'
os.environ['inferschema'] = 'True'

envn = os.environ['envn']
header = os.environ['header']
inferschema = os.environ['inferschema']

appName = 'NycDataCleaning'

current_folder = os.getcwd()

src_path = current_folder + '\Source\input'

output_path = current_folder + '\Source\output'

print(src_path)
print(output_path)


# for ls in os.listdir(src_path):
#     print(ls)
#     file = src_path + '\\' + ls
#     print(file)
#     df = pd.read_csv(file)
#     print(df)
#     print(pd.DataFrame.count(df))
#     print(pd.DataFrame.drop_duplicates(df).count())





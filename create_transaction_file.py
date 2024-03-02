
import pandas as pd
import subprocess

#Tạo folder chứa file tổng
create_folder_command = f"hadoop fs -mkdir /transaction_data"
subprocess.run(create_folder_command, shell=True)

# Define the header and CSV file name
header = "key,User,Card,Date,Time,Amount,Merchant Name,Merchant City"
csv_file_name = "transaction.csv"

# Create a DataFrame with the header
df_header = pd.DataFrame([header.split(',')])

# Save the DataFrame to a CSV file
df_header.to_csv(csv_file_name, index=False, header=False)

hdfs_path = "hdfs://127.0.0.1:9000/transaction_data/"

# Specify the paths
local_csv_path = csv_file_name
hdfs_csv_path = hdfs_path + csv_file_name

# Upload the file to HDFS using subprocess
upload_command = f"hadoop fs -put {local_csv_path} {hdfs_csv_path}"
subprocess.run(upload_command, shell=True, check=True)

print(f"CSV header file '{csv_file_name}' uploaded to HDFS path: '{hdfs_csv_path}'")

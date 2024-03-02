
import subprocess

# Output file
output_file = "hdfs://127.0.0.1:9000/transaction_data/transaction.csv"

# Input directory
input_directory = "hdfs://127.0.0.1:9000/transaction_temp_data/"

# Merge CSV files into the single file
merge_command = f"hadoop fs -cat {input_directory}/part-*.csv | hadoop fs -appendToFile - {output_file}"
subprocess.run(merge_command, shell=True)

# Delete the source files
delete_command = f"hadoop fs -rm {input_directory}/part-*.csv"
subprocess.run(delete_command, shell=True)

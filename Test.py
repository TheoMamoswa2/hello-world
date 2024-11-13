import sys
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import lit
import boto3
import re
from datetime import datetime  # Import datetime for date parsing

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'config_json'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

config = json.loads(args['config_json'])
bucket = args['source_bucket']

s3_client = boto3.client('s3')

for table in config['tables']:
    source_folder = f"s3://{bucket}/{table['source_s3_path']}/"

    # Use Delimiter to get immediate subfolders
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=table['source_s3_path'],
        Delimiter='/'
    )

    if 'CommonPrefixes' in response:
        subfolders = [cp['Prefix'] for cp in response['CommonPrefixes']]
        # Filter subfolders that match 'partday=YYYY-MM-DD/'
        partday_folders = [
            folder for folder in subfolders
            if re.match(r'{0}partday=\d{{4}}-\d{{2}}-\d{{2}}/'.format(table['source_s3_path']), folder)
        ]

        if not partday_folders:
            raise ValueError(f"No partday folder found in {source_folder}.")

        folder_dates = []
        for folder in partday_folders:
            match = re.search(r'partday=(\d{4}-\d{2}-\d{2})/', folder)
            if match:
                part_day = match.group(1)
                # Convert part_day to a date object
                date_obj = datetime.strptime(part_day, '%Y-%m-%d').date()
                folder_dates.append((folder, date_obj))
            else:
                continue  # Skip if date is not found

        if folder_dates:
            # Sort folders by date in descending order to get the latest date
            folder_dates.sort(key=lambda x: x[1], reverse=True)
            latest_folder = folder_dates[0][0]
            part_day = folder_dates[0][1].strftime('%Y-%m-%d')
            new_s3_path = f"s3://{bucket}/{latest_folder}"
        else:
            raise ValueError(f"No valid partday folders found in {source_folder}.")
    else:
        raise ValueError(f"No subfolders found in {source_folder}.")

    # Read S3 Data
    S3Node = glueContext.create_dynamic_frame.from_options(
        format_options={},
        connection_type="s3",
        format="parquet",
        connection_options={"paths": [new_s3_path], "recurse": True},
        transformation_ctx="S3Node"
    )

    df = S3Node.toDF()

    # Apply table mapping
    if 'column_mapping' in table:
        for old_col, new_col in table['column_mapping'].items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)

    df_with_part_day = df.withColumn("part_day", lit(part_day))

    dynamic_frame_with_part_day = DynamicFrame.fromDF(df_with_part_day, glueContext, "dynamic_frame_with_part_day")

    # Write the data to SAP HANA
    SapHanaNode = glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame_with_part_day,
        connection_type="saphana",
        connection_options={
            "dbtable": table['sap_hana_table'],
            "connectionName": "HanaConnection"
        },
        transformation_ctx="SapHanaNode"
    )

job.commit()
job.commit()
job.commit()
new edit lets merge

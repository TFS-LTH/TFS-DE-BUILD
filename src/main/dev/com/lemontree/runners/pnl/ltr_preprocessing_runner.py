from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_helper_methods import get_managed_hotels
from com.lemontree.utils.utils_email import send_email_with_attachment
from datetime import datetime, timedelta
import io
import pandas as pd
import zipfile
import boto3

class LtrPreprocessingRunner(BaseJobRunner):
    def run_job(self, spark_session, glue_context, args) -> None:
        self.logger.info(f"[{LtrPreprocessingRunner.__name__}] Starting Local Job ...")
        run_ltr(spark_session, glue_context, self.config, args)

def run_ltr(spark_session, glue_context, config, args):
    print("Running LTR pipeline...")

    tb_file_path = config.get("tb_file_path")
    mapping_file = config.get("mapping_file")
    portel_bucket_name = config.get("portel_bucket_name")
    bucket_name = config.get("bucket_name")
    parquet_output_path = config.get("parquet_output_path")
    csv_output_path = config.get("csv_output_path")
    notify_email = config.get("notify_email")
    hotel_codes = args['hotel_codes']

    full_parquet_output_path = f"{bucket_name}/{parquet_output_path}"
    full_csv_output_path = f"{bucket_name}/{csv_output_path}"

    print(' ############################### start processing LTR PER PROCESSING ############################### ')

    first_day_of_current_month = datetime.now().replace(day=1)
    first_day_formatted = first_day_of_current_month.strftime("%d%m%Y")
    month_ltr = (datetime.now() - timedelta(days=30)).strftime("%m")
    month_name = (datetime.now() - timedelta(days=30)).strftime("%b")
    year_ltr = (datetime.now() - timedelta(days=30)).strftime("%Y")

    s3_client = boto3.client('s3')

    destination_file_path_parquet = f"{full_parquet_output_path}/year={year_ltr}/month={month_ltr}"
    destination_file_path_csv = f"{full_csv_output_path}/year={year_ltr}/month={month_ltr}"

    # read the tb data for the month to get the hotel codes
    managed_hotels = get_managed_hotels(tb_file_path, hotel_codes)
    error_list = []
    for hotel in managed_hotels:
        # Extract the hotel code
        if '.' in hotel:
            hotel_code = hotel.split('.')[1].split('_')[0]
        else:
            hotel_code = hotel.split('_')[0]

        print(f'Processing hotel_code: {hotel_code}')

        # Replace hotel code if it's LTHJP & LTPAH
        if hotel_code == "LTHJP":
            hotel_code = "LTPJP1"
        elif hotel_code == "LTPAH":
            hotel_code = "LTPAH1"

        ltr_mapping_df = pd.read_csv(mapping_file, delimiter=',')
        ltr_mapping_dict = dict(zip(ltr_mapping_df['code'], ltr_mapping_df['hotel_name']))

        ltr_hotel_name = ltr_mapping_dict[hotel]
        ltr_file_name = f"{ltr_hotel_name}_{first_day_formatted}.csv"
        ltr_file_path = f"{portel_bucket_name}/{ltr_file_name}"

        print(f'Processing ltr_mapping_file: {ltr_file_path}')

        try:
            # Read the file from the source S3 bucket
            df = pd.read_csv(ltr_file_path, delimiter=';')
            df.to_parquet(f'{destination_file_path_parquet}/{hotel_code}.parquet', index=False)
            df.to_csv(f'{destination_file_path_csv}/{hotel_code}.csv', index=False)

            # LTR - to finance
            ltr = pd.read_csv(f'{destination_file_path_csv}/{hotel_code}.csv', delimiter=',')
            # Calculate the sum of "Total Room Revenue"
            total_revenue = ltr['Total Room Revenue'].sum()

            # Append the result to the DataFrame
            new_row = pd.DataFrame([{'Hotel Code': hotel_code, 'Total Room Revenue': total_revenue}])
            ltr_final = pd.concat([ltr_final, new_row], ignore_index=True)

            print(f"Processed for hotel_code: {hotel_code}")

        except Exception as e:
            print(f"Error processing file {hotel_code}: {str(e)}")
            error_list.append(hotel_code)

    if len(error_list) > 0:
        print("Error found while processing LTR the below hotels.")
        for item in error_list:
            print(item)

        send_email_with_attachment(notify_email, None, None,
                                   f"Processing of LTR failed for  hotel codes: {', '.join(error_list)}")
    else:
        print("Processing completed without any errors.")
        ltr_final.to_csv(f'{destination_file_path_csv}/{month_name}_LTR.csv', index=False)

        send_email_with_attachment(notify_email, None, None,
                                   f"Processing of LTR Completed successfully for  hotel codes: {', '.join(managed_hotels)}: .")

    print(' ############################### end processing LTR PER PROCESSING ############################### ')

    ##################################################### ZIPS OF LTR ##############################################################

    print(' ############################### start processing ZIPS OF LTR ############################### ')

    csv_folder_prefix = f"{csv_output_path}/year={year_ltr}/month={month_ltr}/"
    zip_output_key = f"{csv_output_path}/year={year_ltr}/month={month_ltr}/{month_name}_LTR.zip"

    # List all CSV files in the given prefix
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=csv_folder_prefix)
    csv_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

    # Create an in-memory zip file
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for csv_key in csv_keys:
            # Get the file object from S3
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=csv_key)
            csv_data = s3_object['Body'].read()

            # Extract file name from full S3 key
            filename = csv_key.split('/')[-1]

            # Add to zip
            zip_file.writestr(filename, csv_data)

    # Seek to the beginning of the BytesIO buffer
    zip_buffer.seek(0)

    # Upload the ZIP file to S3
    s3_client.upload_fileobj(zip_buffer, bucket_name, zip_output_key)
    print(f"ZIP file uploaded to {bucket_name}/{zip_output_key}")

    send_email_with_attachment(notify_email, None, f"{month_name}_LTR.zip",
                               f"ZIP of LTR Completed successfully for hotel codes: {', '.join(managed_hotels)}: .")

    print(' ############################### end processing ZIPS OF LTR ############################### ')
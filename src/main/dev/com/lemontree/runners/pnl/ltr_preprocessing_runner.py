from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_helper_methods import get_managed_hotels
from com.lemontree.utils.utils_email import send_email_with_attachments
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

    first_day_of_current_month = datetime.now().replace(day=1)
    first_day_formatted = first_day_of_current_month.strftime("%d%m%Y")
    month_ltr = (datetime.now() - timedelta(days=30)).strftime("%m")
    month_name = (datetime.now() - timedelta(days=30)).strftime("%b")
    year_ltr = (datetime.now() - timedelta(days=30)).strftime("%Y")

    print(f"first_day_of_current_month: {first_day_of_current_month}")
    print(f"first_day_formatted: {first_day_formatted}")
    print(f"month_ltr: {month_ltr}")
    print(f"month_name: {month_name}")
    print(f"year_ltr: {year_ltr}")

    # Initialize S3 client
    s3_client = boto3.client('s3')

    bucket_name = config.get("bucket_name")
    portel_bucket_name = config.get("portel_bucket_name")
    bucket_nm = config.get("bucket_name").replace("s3://", "")
    output_prefix = config.get("output_path").strip("/") + "/"
    suffix = '_ltr.xlsx'
    zip_output_key = f"{output_prefix}{month_name}_ltr.zip"
    local_zip_file = f"{month_name}_ltr.zip"

    mapping_file = bucket_name + config.get("mapping_file")
    output_path = bucket_name + config.get("output_path")
    notify_email = config.get("notify_email")
    hotel_codes = args.get('hotel_codes')

    print(f"Output Prefix      : {output_prefix}")
    print(f"Target ZIP Key     : {zip_output_key}")
    print(f"local_zip_file     : {local_zip_file}")

    #destination_file_path = f"{output_path}/year={year_ltr}/month={month_ltr}"

    print(' ############################### start processing LTR PER PROCESSING ############################### ')

    # read the tb data for the month to get the hotel codes
    managed_hotels = get_managed_hotels(hotel_codes)
    error_list = []
    for hotel in managed_hotels:
        # Extract the hotel code
        if '.' in hotel:
            hotel_code = hotel.split('.')[1].split('_')[0]
        else:
            hotel_code = hotel.split('_')[0]

        # Replace hotel code if it's LTHJP & LTPAH & LTHMB1
        if hotel_code == "LTHJP":
            hotel_code = "LTPJP1"
        elif hotel_code == "LTPAH":
            hotel_code = "LTPAH1"
        elif hotel_code == "LTHMB":
            hotel_code = "LTHMB1"

        print(f'Processing hotel_code: {hotel_code}')

        ltr_mapping_df = pd.read_csv(mapping_file)
        ltr_mapping_df.columns = ltr_mapping_df.columns.str.lower()
        ltr_mapping_dict = dict(zip(ltr_mapping_df['hotel_code'], ltr_mapping_df['hotel_name']))

        print(f"ltr_mapping_dict: {ltr_mapping_dict}")

        ltr_hotel_name = ltr_mapping_dict[hotel]
        ltr_file_name = f"{ltr_hotel_name}_{first_day_formatted}.csv"
        ltr_file_path = f"{portel_bucket_name}/{ltr_file_name}"

        print(f'Processing ltr_mapping_file: {ltr_file_path}')

        try:
            # Read the file from the source S3 bucket
            df = pd.read_csv(ltr_file_path, delimiter=';', encoding='ISO-8859-1')
            df.to_excel(f'{output_path}/{hotel_code}{suffix}', index=False)

            print(f"Processed for hotel_code: {hotel_code}")

        except Exception as e:
            print(f"Error processing file {hotel_code}: {str(e)}")
            error_list.append(hotel_code)

    if len(error_list) > 0:
        print("Error found while processing LTR the below hotels.")
        for item in error_list:
            print(item)

        send_email_with_attachments(notify_email, None, None, None, None,
                                   f"Processing of LTR failed for  hotel codes: {', '.join(error_list)}", "LTR Pnl JOB")
    else:
        print("Processing completed without any errors.")


    print(' ############################### end processing LTR PER PROCESSING ############################### ')

    # Initialize the final DataFrame before your loop
    ltr_final = pd.DataFrame(columns=['Hotel Code', 'Total Room Revenue'])

    # List all xlsx files in the output path
    response = s3_client.list_objects_v2(Bucket=bucket_nm, Prefix=output_prefix)
    xlsx_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.xlsx')]

    if not xlsx_keys:
        print("No xlsx files found to zip.")
    else:
        print(f"Found these xlsx_keys files to process: {xlsx_keys}")
        for file_key in xlsx_keys:
            print(f"Reading {file_key}...")
            obj = s3_client.get_object(Bucket=bucket_nm, Key=file_key)

            ltr = pd.read_excel(io.BytesIO(obj['Body'].read()))
            print(f"file_key: {file_key}")

            # Calculate the sum of "Total Room Revenue"
            total_revenue = ltr['Total Room Revenue'].sum()

            # Append the result to the final DataFrame
            new_row = pd.DataFrame([{
                'Hotel Code': file_key.split(suffix)[0].split("/")[-1],
                'Total Room Revenue': total_revenue
            }])
            ltr_final = pd.concat([ltr_final, new_row], ignore_index=True)

        ltr_final.to_excel(f'{output_path}/{month_name}_cumulative_ltrs.xlsx', index=False)
        print(f"File saved: {output_path}/{month_name}_cumulative_ltrs.xlsx")

        print('###################### START: Processing ZIP of LTR files ######################')

    # List all xlsx files in the output path along with cumulative ltrs
    response_cumulative = s3_client.list_objects_v2(Bucket=bucket_nm, Prefix=output_prefix)
    all_keys = [obj['Key'] for obj in response_cumulative.get('Contents', []) if obj['Key'].endswith('.xlsx')]

    if not all_keys:
        print("No xlsx files found to zip.")
    else:
        print(f"Found these xlsx_keys files to process: {all_keys}")
        # Create an in-memory ZIP file
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for xlsx_key in all_keys:
                s3_object = s3_client.get_object(Bucket=bucket_nm, Key=xlsx_key)
                csv_data = s3_object['Body'].read()
                filename = xlsx_key.split('/')[-1]
                zip_file.writestr(filename, csv_data)

        zip_buffer.seek(0)

        # Upload ZIP to S3
        s3_client.upload_fileobj(zip_buffer, bucket_nm, zip_output_key)
        print(f"ZIP uploaded to: s3://{bucket_nm}/{zip_output_key}")

        # Step 4: Download the ZIP locally
        s3_client.download_file(bucket_nm, zip_output_key, local_zip_file)

        with open(local_zip_file, 'rb') as f_zip:
            zip_bytes = f_zip.read()

        send_email_with_attachments(notify_email, None, None, zip_bytes, local_zip_file,
                               f"LTR data of following hotels are present in the zip: {', '.join(managed_hotels)}",
                                "LTR Detailed Report")

    print(' ############################### end processing ZIPS OF LTR ############################### ')
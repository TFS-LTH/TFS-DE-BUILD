from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_helper_methods import get_managed_hotels_from_tb
from com.lemontree.utils.utils_email import send_email_with_attachments
import pandas as pd

class PercentageFeeRunner(BaseJobRunner):
    def run_job(self, spark_session, glue_context,  args) -> None:
        self.logger.info(f"[{PercentageFeeRunner.__name__}] Starting Local Job ...")
        run_percentage_fee(spark_session, glue_context, self.config, args)

def run_percentage_fee(spark_session, glue_context, config, args):
    print("Running run_percentage_fee pipeline...")

    # get all the variable values from config
    bucket_name = config.get("bucket_name")
    mapping_file = config.get("mapping_file")
    output_path = config.get("output_path")
    tb_file_path = config.get("tb_file_path")
    notify_email = config.get("notify_email")
    hotel_codes = args.get('hotel_codes')

    # pre-pare the file paths
    percentage_fee_look_up_path = f"{bucket_name}{mapping_file}"
    full_output_path = f"{bucket_name}/{output_path}"

    print(f"percentage_fee_look_up_path: {percentage_fee_look_up_path}")
    print(f"full_output_path: {full_output_path}")

    # percentage lookup file
    percentage_fee = pd.read_excel(percentage_fee_look_up_path,  header=1)
    percentage_fee = percentage_fee.drop(columns=['Mpehotel'])
    percentage_fee = percentage_fee.rename(columns={'Lemon Tree Smiles (LTS)': 'Lemon Tree Rewards (LTR)'})
    percentage_fee['Particulars'] = 'Percentage'
    percentage_fee = percentage_fee.fillna(0)
    percentage_fee['Incentive Fees:'] = 0
    resulted_columns = ['hotel_code', 'Particulars', 'Management Fees', 'Sales & Marketing Expenses',
                        'Lemon Tree Rewards (LTR)', 'Incentive Fees:', 'GOP % - 0 - 20%',
                        'GOP % - Above 20% upto 25%', 'GOP % - Above 25% upto 30%',
                        'GOP % - Above 30% upto 35%', 'GOP % - Above 35% upto 40%',
                        'GOP % - Above 40% upto 45%', 'GOP % - Above 45% upto 50%',
                        'GOP % - Above 50% upto 55%', 'GOP % - Above 55%', 'Reservation Fees']
    percentage_fee = percentage_fee[resulted_columns]

    error_list = []

    # read the tb data for the month to get the hotel codes
    managed_hotels = get_managed_hotels_from_tb(tb_file_path, hotel_codes)
    for code in managed_hotels:
        try:
            print(f'running percentage_fee for {code}')
            final_per = percentage_fee[percentage_fee['hotel_code'] == code]
            final_per = final_per.drop(columns=['hotel_code'])

            # Transposing
            per_t = final_per.T
            per_t = per_t.reset_index()

            per_t.columns = per_t.iloc[0]
            per_t = per_t.drop(per_t.index[0])
            per_t['Percentage'] = per_t.apply(
                lambda row: '' if row['Particulars'] == 'Incentive Fees: ' else row['Percentage'], axis=1)

            # write in parquet
            per_t.to_parquet(f'{full_output_path}/{code}_percentage_fees.parquet')

            # Create the Excel file name for the final_tb
            excel_file_name = f'{code}_percentage_fees.xlsx'
            per_t.to_excel(f'{full_output_path}/{excel_file_name}' , index=False, sheet_name=f'{code}')

            print(f"Excel file successfully uploaded to {full_output_path}/{excel_file_name}")
        except Exception as e:
            print(f"Error processing percentage fee for hotel_code - {code}: {e}")
            error_list.append(code)

    if len(error_list) > 0:
        print("Error found while processing percentage fee for the below hotels.")
        for item in error_list:
            print(item)
        send_email_with_attachments(notify_email, None, None, None, None,
                                   f"Processing of Percentage Fees failed for  hotel codes: {', '.join(error_list)}", "Percentage Fee Job")

    else:
        print("No error found while processing percentage fees.")

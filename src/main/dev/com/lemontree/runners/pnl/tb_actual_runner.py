from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_helper_methods import get_managed_hotels_from_tb, read_tb_file
from com.lemontree.utils.utils_email import send_email_with_attachments
import pandas as pd
from datetime import datetime, timedelta

class TbActualRunner(BaseJobRunner):
    def run_job(self, spark_session, glue_context,  args) -> None:
        self.logger.info(f"[{TbActualRunner.__name__}] Starting Local Job ...")
        run_tb_actual(spark_session, glue_context, self.config, args)

# Function to remove extra spaces in all string columns
def remove_extra_spaces(df):
    return df.apply(lambda x: x.str.replace(r'\s+', ' ', regex=True) if x.dtype == "object" else x)


def run_tb_actual(spark_session, glue_context, config, args):
    print("Running tb_actual pipeline...")

    # inputs
    tb_file_name = args['tb_file_name']
    hotel_codes = args['hotel_codes']
    update_month = args.get('update_month', '').strip()

    # get all the variable values from config
    bucket_name = config.get("bucket_name")
    back_up_tb_path = bucket_name + config.get("back_up_tb_path")
    tb_source_path = config.get("tb_source_path")
    tb_file_path = bucket_name + config.get("tb_file_path")
    fsli_path = bucket_name + config.get("flsi_mapping_path")
    output_path = bucket_name + config.get("output_path")
    notify_email = config.get("notify_email")

    print(f"bucket_name : {bucket_name}")
    print(f"back_up_tb_path : {back_up_tb_path}")
    print(f"tb_source_path : {tb_source_path}")
    print(f"tb_file_path : {tb_file_path}")
    print(f"fsli_path : {fsli_path}")
    print(f"output_path : {output_path}")
    print(f"notify_email : {notify_email}")

    # Date handling
    if update_month == "":
        # First day of previous month (accurate)
        first_day_this_month = datetime.now().replace(day=1)
        last_month_date = (first_day_this_month - timedelta(days=1)).replace(day=1)
    else:
        # Parse the input string to datetime
        try:
            last_month_date = datetime.strptime(update_month, "%Y-%m")  # or another format you expect
        except ValueError:
            raise ValueError(f"Invalid date format: {update_month}. Expected 'YYYY-MM'.")

    print(f"last_month_date: {last_month_date}")
    backup_timestamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
    print(f'backup_timestamp: {backup_timestamp}')

    last_month_year = last_month_date.strftime("%b-%y").capitalize()

    # Current year in YY format
    current_year_yy = datetime.now().strftime("%y")
    # Full current year (YYYY)
    current_year_full = datetime.now().year
    # Future year (next year)
    future_year_full = current_year_full + 1
    # Future year in YY format
    future_year_yy = str(future_year_full)[2:]
    # Previous year (YY)
    previous_year_yy = f"{(current_year_full - 1) % 100:02d}"
    two_years_ago_yy = f"{(current_year_full - 2) % 100:02d}"

    # backup directory
    backup_path = f'{back_up_tb_path}/{backup_timestamp}'

    # this file has all hotel tb data for the month
    full_file_name = f'{tb_file_name}.csv'
    # read the tb data for the month
    tb_path = f'{tb_file_path}/{full_file_name}'
    print(f'tb_path : {tb_path}')

    tb_path_full = f'{tb_source_path}/{full_file_name}'
    print(f'tb_path_full : {tb_path_full}')
    # copy tb from source and pace in our warehouse
    tb = pd.read_csv(tb_path_full, encoding='ISO-8859-1', header=2)
    tb.to_csv(tb_path, index=False)
    tb_full = read_tb_file(tb_path)

    # Loading FSLI mapping
    fsli_mapping = pd.read_excel(fsli_path)
    fsli_mapping['GL Code'] = fsli_mapping['GL Code'].astype(str)
    fsli_mapping['GL Code'] = fsli_mapping['GL Code'].str.strip()
    fsli_mapping['GL Name'] = fsli_mapping['GL Name'].str.strip()
    fsli_mapping['GL Code & Name'] = fsli_mapping['GL Code & Name'].str.strip()
    fsli_mapping = remove_extra_spaces(fsli_mapping)

    # list to hold all the error hotels
    error_list = []

    # read the tb data for the month to get the hotel codes
    managed_hotels = get_managed_hotels_from_tb(tb_path, hotel_codes)
    for code in managed_hotels:
        print("RUNNING FOR :  ", code, tb_path)

        try:
            tb = tb_full[tb_full['Abbrevation'] == code].copy()
            print(f'file filtered successfully for hotel_code : ' + code)

            tb = tb.rename(columns={'Amount': f'{last_month_year}'})
            tb = tb[['GL Code', 'GL Name', f'{last_month_year}']]
            tb['GL Code'] = tb['GL Code'].str.strip('"=')
            tb['GL Name'] = tb['GL Name'].str.strip()
            tb['GL Code & Name'] = tb['GL Code'].astype(str) + ' ' + tb['GL Name']
            tb['GL Code & Name'] = tb['GL Code & Name'].str.strip()
            tb[f'{last_month_year}'] = tb[f'{last_month_year}'].astype('float64')
            tb[f'{last_month_year}'] = tb[f'{last_month_year}'].fillna(0)
            tb_final = tb[['GL Code', 'GL Name', f'{last_month_year}', 'GL Code & Name']]

            # Removing extra spaces from GL name and GL code and Name Column
            tb_final = remove_extra_spaces(tb_final)
            tb_final = tb_final.groupby(['GL Code & Name', 'GL Code', 'GL Name'])[
                [f'{last_month_year}']].sum().reset_index()
            print('Final tb for the month created for hotel_code: ' + code)

            # Join to fsli
            final_tb = pd.merge(fsli_mapping, tb_final.drop(['GL Name', 'GL Code & Name'], axis=1), on=['GL Code'], how='right')
            print(f'joined with fsli for hotel_code: ' + code)

            # change the expence columns to positive
            final_tb['FS Mapping'] = final_tb['FS Mapping'].str.strip()
            final_tb['FSLI Mapping'] = final_tb['FSLI Mapping'].str.strip()

            month_col = f"{last_month_year}"
            final_tb[month_col] = pd.to_numeric(final_tb[month_col], errors='coerce')

            # Rule 1: Expense rows
            expense_mask = final_tb['FS Mapping'] == 'Expenses'
            final_tb.loc[expense_mask, month_col] = final_tb.loc[expense_mask, month_col].abs()

            # Rule 2: Service Charge Expense & Income
            special_mask = (
                    (final_tb['FSLI Mapping'] == 'Service Charge Expense') &
                    (final_tb['FS Mapping'] == 'Income')
            )
            final_tb.loc[special_mask, month_col] = final_tb.loc[special_mask, month_col].abs()

            # Taking last month Tb and then merging with current TB
            complied_tb_file_name = output_path + f'/{code}_tb_actuals.xlsx'
            print(f'reading file: {complied_tb_file_name}')

            last_tb = pd.read_excel(complied_tb_file_name)
            # keep a backup to this directory
            complete_backup_path = backup_path + f'/{code}_tb_actuals.xlsx'
            last_tb.to_excel(complete_backup_path, index=False, sheet_name=f'{code}')
            print(f'Back up created at : {complete_backup_path}')

            last_tb = last_tb.drop(columns=[f'{last_month_year}'])
            print(f"dropped column {last_month_year}")

            resulted_tb = last_tb.merge(final_tb,
                                        on=['GL Code & Name', 'GL Code', 'GL Name', 'FSLI Mapping', 'FS Category',
                                            'FS Mapping',
                                            'Department Profitability'], how='outer')

            resulted_fsli_columns = ['Department Profitability', 'FS Mapping', 'FS Category', 'FSLI Mapping',
                                     'GL Code & Name', 'GL Code', 'GL Name', f'Apr-{two_years_ago_yy}',
                                     f'May-{two_years_ago_yy}',
                                     f'Jun-{two_years_ago_yy}', f'Jul-{two_years_ago_yy}', f'Aug-{two_years_ago_yy}',
                                     f'Sep-{two_years_ago_yy}', f'Oct-{two_years_ago_yy}', f'Nov-{two_years_ago_yy}',
                                     f'Dec-{two_years_ago_yy}',
                                     f'Jan-{previous_year_yy}', f'Feb-{previous_year_yy}', f'Mar-{previous_year_yy}',
                                     f'Apr-{previous_year_yy}', f'May-{previous_year_yy}', f'Jun-{previous_year_yy}',
                                     f'Jul-{previous_year_yy}', f'Aug-{previous_year_yy}', f'Sep-{previous_year_yy}',
                                     f'Oct-{previous_year_yy}', f'Nov-{previous_year_yy}', f'Dec-{previous_year_yy}',
                                     f'Jan-{current_year_yy}', f'Feb-{current_year_yy}', f'Mar-{current_year_yy}',
                                     f'Apr-{current_year_yy}', f'May-{current_year_yy}', f'Jun-{current_year_yy}',
                                     f'Jul-{current_year_yy}', f'Aug-{current_year_yy}', f'Sep-{current_year_yy}',
                                     f'Oct-{current_year_yy}', f'Nov-{current_year_yy}', f'Dec-{current_year_yy}',
                                     f'Jan-{future_year_yy}', f'Feb-{future_year_yy}', f'Mar-{future_year_yy}']
            resulted_tb = resulted_tb[resulted_fsli_columns]
            resulted_tb = resulted_tb.dropna(subset=['GL Code & Name'])
            fillna_columns = [f'Apr-{two_years_ago_yy}', f'May-{two_years_ago_yy}', f'Jun-{two_years_ago_yy}',
                              f'Jul-{two_years_ago_yy}', f'Aug-{two_years_ago_yy}', f'Sep-{two_years_ago_yy}',
                              f'Oct-{two_years_ago_yy}', f'Nov-{two_years_ago_yy}', f'Dec-{two_years_ago_yy}',
                              f'Jan-{previous_year_yy}', f'Feb-{previous_year_yy}', f'Mar-{previous_year_yy}',
                              f'Apr-{previous_year_yy}', f'May-{previous_year_yy}', f'Jun-{previous_year_yy}',
                              f'Jul-{previous_year_yy}', f'Aug-{previous_year_yy}', f'Sep-{previous_year_yy}',
                              f'Oct-{previous_year_yy}', f'Nov-{previous_year_yy}', f'Dec-{previous_year_yy}',
                              f'Jan-{current_year_yy}', f'Feb-{current_year_yy}', f'Mar-{current_year_yy}',
                              f'Apr-{current_year_yy}', f'May-{current_year_yy}', f'Jun-{current_year_yy}',
                              f'Jul-{current_year_yy}', f'Aug-{current_year_yy}', f'Sep-{current_year_yy}',
                              f'Oct-{current_year_yy}', f'Nov-{current_year_yy}', f'Dec-{current_year_yy}',
                              f'Jan-{future_year_yy}', f'Feb-{future_year_yy}', f'Mar-{future_year_yy}']
            resulted_tb[fillna_columns] = resulted_tb[fillna_columns].astype('float64')
            resulted_tb[fillna_columns] = resulted_tb[fillna_columns].fillna(0)

            # save this in parquet file as lambda jobs are set to pull this files.
            # resulted_tb.to_parquet(parquet_output_path + f'/{code}_tb_actuals.parquet')

            # Create the Excel file name for the final_tb
            final_file_path = f'{output_path}/{code}_tb_actuals.xlsx'
            # Save the final_tb DataFrame to an Excel file in memory
            resulted_tb.to_excel(final_file_path, index=False, sheet_name=f'{code}')
            print(f"Excel file successfully saved to {final_file_path}")

        except Exception as e:
            print(f"Error found while processing tb for hotel_code: {code}. Error: ", e)
            error_list.append(code)

    if len(error_list) > 0:
        print("################################################### Error found while processing tb files. ###############################################################")
        for item in error_list:
            print(item)

        send_email_with_attachments(notify_email, None, None, None, None,
                                    f"Processing of TB Actuals failed for  hotel codes: {', '.join(error_list)}")

    else:
        print('No Errors found')
        send_email_with_attachments(notify_email, None, None, None, None,
                                    f"Processing of TB Actuals was successful for hotel codes: {', '.join(managed_hotels)}")



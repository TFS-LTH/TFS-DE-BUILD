from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_helper_methods import get_managed_hotels_from_tb
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, first, date_format, to_date
from pyspark.sql.types import DateType

class OperationalDataRunner(BaseJobRunner):
    def run_job(self, spark_session, glue_context,  args) -> None:
        self.logger.info(f"[{OperationalDataRunner.__name__}] Starting Local Job ...")
        run_operational_data(spark_session, glue_context, self.config, args)

# this manipulation are done as hotel_codes are different between protel and tb
def convert_hotel_code(code):
    if code in ['LTPJP1', 'LTPAH1', 'LTHMB1', 'LTPPT1']:
        return code[:-1]  # Remove the last '1'
    return code

def run_operational_data(spark_session, glue_context, config, args):
    print("Running operational_data pipeline...")

    update_month = args.get('update_month', '').strip()
    tb_file_path = config.get("tb_file_path")
    hotel_codes = args.get('hotel_codes')

    # s3_bucket_name = 'ltree-softsensor-dashboards'
    dbr_bucket = config.get("dbr_bucket")
    bucket_name = config.get("bucket_name")
    budget_file = config.get("budget_file")
    ltr_output_file_path = bucket_name + config.get("ltr_output")
    operational_data_actual = bucket_name + config.get("operational_data_actual")
    operational_data_budget = bucket_name + config.get("operational_data_budget")
    back_up_operational_data_path = bucket_name + config.get("back_up_operational_data_path")

    print(f"dbr_bucket - {dbr_bucket}")
    print(f"bucket_name - {bucket_name}")
    print(f"budget_file - {budget_file}")
    print(f"ltr_output_file_path - {ltr_output_file_path}")
    print(f"operational_data_actual - {operational_data_actual}")
    print(f"operational_data_budget - {operational_data_budget}")
    print(f"back_up_operational_data_path - {back_up_operational_data_path}")


    # Date handling
    if update_month == "":
        # First day of this month
        first_day_this_month = datetime.now().replace(day=1)
        # Last day of previous month = 1st of this month - 1 day
        last_month_date = first_day_this_month - timedelta(days=1)
    else:
        # Parse the input string to datetime
        try:
            last_month_date = datetime.strptime(update_month, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format: {update_month}. Expected 'YYYY-MM-dd'.")

    month = last_month_date.strftime("%m")
    year = last_month_date.strftime("%Y")
    last_day_formatted = last_month_date.strftime("%Y-%m-%d")
    last_month_year = last_month_date.strftime("%b-%y").capitalize()
    current_year_yy = datetime.now().strftime("%y")
    current_year_full = datetime.now().year
    future_year_full = current_year_full + 1
    future_year_yy = str(future_year_full)[2:]
    previous_year_yy = f"{(current_year_full - 1) % 100:02d}"
    two_years_ago_yy = f"{(current_year_full - 2) % 100:02d}"
    backup_timestamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

    print(f'Running for the date: {last_month_date}')
    print(f'month: {month}')
    print(f'year: {year}')
    print(f'last_day_formatted: {last_day_formatted}')
    print(f'last_month_year: {last_month_year}')
    print(f'current_year_yy: {current_year_yy}')
    print(f'current_year_full: {current_year_full}')
    print(f'future_year_full: {future_year_full}')
    print(f'future_year_yy: {future_year_yy}')
    print(f'previous_year_yy: {previous_year_yy}')
    print(f'two_years_ago_yy: {two_years_ago_yy}')
    print(f'backup_timestamp: {backup_timestamp}')

    # backup directory
    backup_path = f'{back_up_operational_data_path}/{backup_timestamp}'


    dbr = spark_session.read.parquet(f'{dbr_bucket}/production/dbr_output/dbr_warehouse/')
    print(f"last_day_formatted - {last_day_formatted}")
    dbr_fil = dbr.filter((F.col('Date_for_Dash') == f'{last_day_formatted}') & (F.col('Owned_vs_Managed') == 'Managed')). \
        select('Hotel_Code', 'No_of_rooms', 'Category', 'Date_For_Dash', 'MTD', 'MTD_Night')

    dbr_fil = dbr_fil.withColumn('date_for_dash', F.col('date_for_dash').cast(DateType()))

    # Extract year and month
    dbr_fil = dbr_fil.withColumn('year', F.year(F.col('date_for_dash')))
    dbr_fil = dbr_fil.withColumn('month', F.month(F.col('date_for_dash')))
    dbr_fil = dbr_fil.withColumn('day', F.dayofmonth(F.col('date_for_dash')))

    # last day of each month
    dbr_fil = dbr_fil.withColumn('last_day_of_month', F.expr("last_day(date_for_dash)"))

    # Filter rows where date_for_dash is the last day of the month
    filtered_dbr = dbr_fil.filter(F.col('date_for_dash') == F.col('last_day_of_month'))

    filtered_dbr = filtered_dbr.filter(F.col('Category').isin('2022', 'Total Revenue', ' # Service Charge'))

    # Calculate the 'room night sold from 2022' column
    filtered_dbr = filtered_dbr.withColumn('value', F.col('MTD_Night') * F.col('day'))
    filtered_dbr = filtered_dbr.withColumn('final_value', F.col('value') + F.col('MTD'))
    filtered_dbr = filtered_dbr.select('Hotel_Code', 'No_of_rooms', 'Category', 'final_value', 'day', 'date_for_dash')
    filtered_dbr = filtered_dbr.withColumn('Room Nights Available', F.col('No_of_rooms') * F.col('day'))
    filtered_dbr = filtered_dbr.withColumn("Month-Year",
                                           date_format(to_date(F.col("date_for_dash"), "yyyy-MM-dd"), "MMM-yy"))

    new_category_df = filtered_dbr.select(
        'Hotel_Code',
        lit(F.col('No_of_rooms')).alias('No_of_rooms'),
        lit('Room Nights Available').alias('Category'),
        F.col('Room Nights Available').alias('final_value'),
        'day',
        'Month-Year'
    )
    new_category_df = new_category_df.dropDuplicates(['Hotel_Code', 'Category', 'Month-Year'])

    # inventory
    inventory = filtered_dbr.select(
        'Hotel_Code',
        lit('No_of_rooms').alias('Category'),
        F.col('No_of_rooms').alias('Inventory'),
        'Month-Year'
    )

    final_dbr = filtered_dbr.drop('Room Nights Available', 'date_for_dash')
    final_dbr = final_dbr.union(new_category_df)

    # Pivoting
    pivot_df = final_dbr.groupBy("Hotel_Code", "Category").pivot("Month-Year").agg(first("final_value"))

    # inventory
    inventory_df = inventory.groupBy("Hotel_Code", "Category").pivot("Month-Year").agg(first("Inventory"))

    # Converting to Pandas for further transformation
    dbr_pandas = pivot_df.toPandas()
    inventory_pandas = inventory_df.toPandas()

    # print(dbr_pandas.head())

    ######################################################## Operational Actual #######################################################################

    print('##################################### Operational Actual START #######################################')

    # sequence of columns
    resulted_columns = ['Hotel_Code', 'Category', f'Apr-{two_years_ago_yy}', f'May-{two_years_ago_yy}',
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
    error_list = []

    managed_hotels = get_managed_hotels_from_tb(tb_file_path, hotel_codes)

    for code in managed_hotels:
        # read the tb data for the month to get the hotel codes
        try:
            print(f'Running operational Increment for hotel: {code}')
            if code in ['LTPPT', 'LTHMB']:
                code = f'{code}1'

            hotel_data = dbr_pandas[dbr_pandas['Hotel_Code'] == code]
            inventory_data = inventory_pandas[inventory_pandas['Hotel_Code'] == code]
            inventory_data = inventory_data.replace('No_of_rooms', 'Inventory')
            hotel_data = hotel_data.drop(columns=['Hotel_Code'])

            # LTR data
            # List of months from Apr-23 to Jul-24
            months = [f'{last_month_year}']

            # Initialize a dictionary to store LTR data and sum of 'Room' amounts
            LTR_data = {}
            LTR_amounts = {}

            # Loop through each month
            for i, month in enumerate(months, start=1):

                converted_code = convert_hotel_code(code)

                # Changing hotel code as requird
                try:
                    # First try to load the sheet as converted name
                    LTR_data[f'LTR{i}'] = pd.read_csv(f'{ltr_output_file_path}/year={year}/month={month}/{converted_code}.csv')
                except Exception as e:
                    # If that fails, try to load the sheet as 'regular name'
                    try:
                        LTR_data[f'LTR{i}'] = pd.read_csv(f'{ltr_output_file_path}/year={year}/month={month}/{code}.csv')
                    except Exception as e1:
                        # Handle the case where both sheet names fail (optional)
                        print(f"Error: Could not find sheet for {month}")
                        continue

                # Drop rows where 'Inv. Number' is NaN
                LTR_data[f'LTR{i}'] = LTR_data[f'LTR{i}'].dropna(subset=['Invoice Number'])

                # Calculate the sum of the 'Room' column
                LTR_amounts[f'LTR_amount{i}'] = LTR_data[f'LTR{i}']['Total Room Revenue'].sum()

            # Assume hotel_data is already defined elsewhere
            # Transpose the data
            transpose = hotel_data.T
            transpose.columns = transpose.iloc[0]
            transpose = transpose.drop(transpose.index[0])

            # Calculate Revenue as per backend file (excluding SC)
            transpose['Revenue as per backend file (excl SC)'] = transpose['Total Revenue'].astype(float) - transpose[' # Service Charge'].astype(float)

            # Drop unnecessary columns
            transpose = transpose.drop(columns=[' # Service Charge', 'Total Revenue'])

            # Add new columns and initialize them to 0
            transpose[['Room Revenue (loyalty data)', 'Total Revenue excluding taxes (Reservation Data)']] = 0

            # Update the transpose dataframe with Room Revenue data for each corresponding month
            for i, month in enumerate(months, start=1):
                transpose.loc[month, 'Room Revenue (loyalty data)'] = LTR_amounts.get(f'LTR_amount{i}', 0)

            # Again transposing to get into final structure
            result = transpose.T
            result = result.reset_index()
            result = result.replace({'2022': 'Room Nights sold'})
            result['Hotel_Code'] = code

            final = pd.concat([inventory_data, result], ignore_index=True)
            final = final.fillna(0)

            # Reading last month
            last_month = pd.read_excel(f'{operational_data_actual}/{code}_operational_data_actual.xlsx')

            # keep a backup to this directory
            complete_backup_path1 = backup_path + f'/actual/{code}_operational_data.xlsx'
            last_month.to_excel(complete_backup_path1, index=False, sheet_name=f'{code}')
            print(f'Back up created at : {complete_backup_path1}')

            last_month = last_month.drop(columns=[f'{last_month_year}'])

            result = last_month.merge(final, on=['Hotel_Code', 'Category'], how='outer')

            operational_data = result[resulted_columns]

            # Create the Excel file name for the final_tb
            excel_file_name = f'/{code}_operational_data_actual.xlsx'
            operational_data.to_excel(operational_data_actual + excel_file_name, sheet_name=f'{code}', index=False)

        except Exception as e:
            print(f"Error found while processing hotel_code: {code}. Error: ", e)
            error_list.append(code)

    if len(error_list) > 0:
        print("Error found while processing the below files.")
        for item in error_list:
            print(item)
    else:
        print('No Errors found')

    print('##################################### Operational Actual END #######################################')

    ############################################################# Operational Budget #######################################################

    print('##################################### Operational Budget START #######################################')

    budget = pd.read_csv(f'{bucket_name}{budget_file}')
    budget = budget.drop(columns=['Mpehotel', 'Match Column', 'FTY'])

    budget_df = budget[budget['Sub_Head'] == 'RPD']
    budget_df = budget_df.rename(columns={'Apr': f'Apr-{current_year_yy}',
                                          'May': f'May-{current_year_yy}',
                                          'Jun': f'Jun-{current_year_yy}',
                                          'Jul': f'Jul-{current_year_yy}',
                                          'Aug': f'Aug-{current_year_yy}',
                                          'Sep': f'Sep-{current_year_yy}',
                                          'Oct': f'Oct-{current_year_yy}',
                                          'Nov': f'Nov-{current_year_yy}',
                                          'Dec': f'Dec-{current_year_yy}',
                                          'Jan': f'Jan-{future_year_yy}',
                                          'Feb': f'Feb-{future_year_yy}',
                                          'Mar': f'Mar-{future_year_yy}'})
    budget_df = budget_df.rename(columns={'HOTEL': 'Hotel_Code', 'Sub_Head': 'Category'})
    budget_df[f'Apr-{current_year_yy}'] = budget_df[f'Apr-{current_year_yy}'].astype('float') * 30
    budget_df[f'May-{current_year_yy}'] = budget_df[f'May-{current_year_yy}'].astype('float') * 31
    budget_df[f'Jun-{current_year_yy}'] = budget_df[f'Jun-{current_year_yy}'].astype('float') * 30
    budget_df[f'Jul-{current_year_yy}'] = budget_df[f'Jul-{current_year_yy}'].astype('float') * 31
    budget_df[f'Aug-{current_year_yy}'] = budget_df[f'Aug-{current_year_yy}'].astype('float') * 31
    budget_df[f'Sep-{current_year_yy}'] = budget_df[f'Sep-{current_year_yy}'].astype('float') * 30
    budget_df[f'Oct-{current_year_yy}'] = budget_df[f'Oct-{current_year_yy}'].astype('float') * 31
    budget_df[f'Nov-{current_year_yy}'] = budget_df[f'Nov-{current_year_yy}'].astype('float') * 30
    budget_df[f'Dec-{current_year_yy}'] = budget_df[f'Dec-{current_year_yy}'].astype('float') * 31
    budget_df[f'Jan-{future_year_yy}'] = budget_df[f'Jan-{future_year_yy}'].astype('float') * 31
    budget_df[f'Feb-{future_year_yy}'] = budget_df[f'Feb-{future_year_yy}'].astype('float') * 28
    budget_df[f'Mar-{future_year_yy}'] = budget_df[f'Mar-{future_year_yy}'].astype('float') * 31
    nights_available = dbr_pandas[dbr_pandas['Category'] == 'Room Nights Available']

    month_columns = [f'Apr-{current_year_yy}', f'May-{current_year_yy}', f'Jun-{current_year_yy}',
                     f'Jul-{current_year_yy}', f'Aug-{current_year_yy}', f'Sep-{current_year_yy}',
                     f'Oct-{current_year_yy}', f'Nov-{current_year_yy}', f'Dec-{current_year_yy}',
                     f'Jan-{future_year_yy}', f'Feb-{future_year_yy}', f'Mar-{future_year_yy}']
    inventory_pandas.loc[inventory_pandas['Hotel_Code'] == 'LTSSN', month_columns] = 195

    updated_nights_available = inventory_pandas.copy()

    # Have to perform to make budgeted inventory of LTSSN to 195 and then multiple it by no. of days
    days_in_month = {
        f'Apr-{previous_year_yy}': 30,
        f'May-{previous_year_yy}': 31,
        f'Jun-{previous_year_yy}': 30,
        f'Jul-{previous_year_yy}': 31,
        f'Aug-{previous_year_yy}': 31,
        f'Sep-{previous_year_yy}': 30,
        f'Oct-{previous_year_yy}': 31,
        f'Nov-{previous_year_yy}': 30,
        f'Dec-{previous_year_yy}': 31,
        f'Jan-{current_year_yy}': 31,
        f'Feb-{current_year_yy}': 29,
        f'Mar-{current_year_yy}': 31,
        f'Apr-{current_year_yy}': 30,
        f'May-{current_year_yy}': 31,
        f'Jun-{current_year_yy}': 30,
        f'Jul-{current_year_yy}': 31,
        f'Aug-{current_year_yy}': 31,
        f'Sep-{current_year_yy}': 30,
        f'Oct-{current_year_yy}': 31,
        f'Nov-{current_year_yy}': 30,
        f'Dec-{current_year_yy}': 31,
        f'Jan-{future_year_yy}': 31,
        f'Feb-{future_year_yy}': 28,
        f'Mar-{future_year_yy}': 31
    }
    for col in days_in_month:
        if col in updated_nights_available.columns:
            updated_nights_available[col] = updated_nights_available[col] * days_in_month[col]

    updated_nights_available = updated_nights_available.replace({'No_of_rooms': 'Room Nights Available'})

    budget_final = pd.concat([budget_df, inventory_pandas, updated_nights_available], ignore_index=True)
    budget_final = budget_final.replace({'RPD': 'Room Nights sold', 'No_of_rooms': 'Inventory'})
    budget_final = budget_final[['Hotel_Code', 'Category', f'{last_month_year}']]

    # Loop through each hotel for budget numbers
    for code in managed_hotels:

        # hack to match the hotel codes
        if code in ['LTPPT', 'LTHMB']:
            code = f'{code}1'

        final = budget_final[budget_final['Hotel_Code'] == code]
        final = final.fillna(0)

        last_month = pd.read_excel(f'{operational_data_budget}/{code}_budget_data.xlsx')

        # keep a backup to this directory
        complete_backup_path = backup_path + f'/budget/{code}_budget_data.xlsx'
        last_month.to_excel(complete_backup_path, index=False, sheet_name=f'{code}')
        print(f'Back up created at : {complete_backup_path}')

        last_month = last_month.drop(columns=[f'{last_month_year}'])

        result = last_month.merge(final, on=['Hotel_Code', 'Category'], how='outer')

        budget_data = result[resulted_columns]

        # Create the Excel file name for the final_tb
        excel_file_name = f'/{code}_budget_data.xlsx'
        budget_data.to_excel(operational_data_budget + excel_file_name, sheet_name=f'{code}', index=False)

    print('##################################### Operational Budget END #######################################')
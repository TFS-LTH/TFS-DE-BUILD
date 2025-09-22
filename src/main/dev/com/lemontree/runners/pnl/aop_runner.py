from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_email import send_email_with_attachments
import pandas as pd
from datetime import datetime, timedelta

class AOPRunner(BaseJobRunner):
    def run_job(self, spark_session, glue_context,  args) -> None:
        self.logger.info(f"[{AOPRunner.__name__}] Starting Local Job ...")
        run_aop(spark_session, glue_context, self.config, args)


def run_aop(spark_session, glue_context, config, args):
    print("Running aop pipeline...")

    hotel_codes = args['hotel_codes']
    managed_hotels = [i.strip() for i in hotel_codes.split(",")]
    print(f'managed_hotels: {managed_hotels}')

    current_year = (datetime.now() - timedelta(days=30)).strftime("%Y")
    backup_timestamp = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

    bucket_name = config.get("bucket_name")
    expense_file_path = bucket_name + config.get("expense_mapping")
    archive_gl_level_budget_path = bucket_name + config.get("archive_gl_level_budget_path")
    income_file_path = bucket_name + config.get("income_mapping")
    fsli_file_path = bucket_name + config.get("flsi_mapping_path")
    gl_level_budget_path = bucket_name + config.get("gl_level_budget_path")
    budget_file_path = bucket_name + config.get("budget_file_path")
    budget_file_path = f'{budget_file_path}/year_{current_year}/'
    notify_email = config.get("notify_email")

    print(f"bucket_name: {bucket_name}")
    print(f"expense_file_path: {expense_file_path}")
    print(f"income_file_path: {income_file_path}")
    print(f"archive_gl_level_budget_path: {archive_gl_level_budget_path}")
    print(f"fsli_file_path: {fsli_file_path}")
    print(f"gl_level_budget_path: {gl_level_budget_path}")
    print(f"budget_file_path: {budget_file_path}")
    print(f"notify_email: {notify_email}")

    back_up_gl_level_budget_path = f'{archive_gl_level_budget_path}/{backup_timestamp}'
    print(f'back_up_gl_level_budget_path: {back_up_gl_level_budget_path}')

    # FSLI MAPPING
    fsli_mapping = pd.read_excel(f'{fsli_file_path}')
    fsli_mapping['GL Code'] = fsli_mapping['GL Code'].str.strip()
    fsli_mapping['GL Name'] = fsli_mapping['GL Name'].str.strip()
    fsli_mapping['GL Code & Name'] = fsli_mapping['GL Code & Name'].str.strip()

    # list to hold all the error hotels
    error_list = []

    for code in managed_hotels:
        hotel_data = [
            (code, f'{budget_file_path}{code}_aop.xlsx')
        ]

        try:
            # Read and process data for each hotel
            AOP = pd.DataFrame()
            expense = pd.DataFrame()
            resulted_bud = pd.DataFrame()
            for hotel_code, file_path in hotel_data:
                # Expense Budget
                print(f"reading for expense sheet for hotel_code: {hotel_code}, path: {file_path}")
                df = pd.read_excel(file_path, sheet_name='Expense')
                hotel = hotel_code
                expense = pd.concat([expense, df])

                if hotel_code != 'LTHDXB1':
                    print('in if')
                    expense = expense[
                        ['Expense forecast (Rs.)', 'Unnamed: 3', 'Unnamed: 24', 'Unnamed: 27', 'Unnamed: 30',
                         'Unnamed: 33',
                         'Unnamed: 36', 'Unnamed: 39', 'Unnamed: 42', 'Unnamed: 45', 'Unnamed: 48', 'Unnamed: 51',
                         'Unnamed: 54', 'Unnamed: 57']]
                    expense = expense.drop([0, 1])
                    expense['Expense forecast (Rs.)'] = expense['Expense forecast (Rs.)'].str.strip()
                    expense = expense.dropna(subset=['Expense forecast (Rs.)'])
                    expense = expense.rename(
                        columns={'Expense forecast (Rs.)': 'Sub_Head', 'Unnamed: 3': 'FTY', 'Unnamed: 24': 'Apr-25',
                                 'Unnamed: 27': 'May-25', 'Unnamed: 30': 'Jun-25',
                                 'Unnamed: 33': 'Jul-25', 'Unnamed: 36': 'Aug-25',
                                 'Unnamed: 39': 'Sep-25', 'Unnamed: 42': 'Oct-25',
                                 'Unnamed: 45': 'Nov-25', 'Unnamed: 48': 'Dec-25',
                                 'Unnamed: 51': 'Jan-26', 'Unnamed: 54': 'Feb-26',
                                 'Unnamed: 57': 'Mar-26'})
                else:
                    print('in else')
                    expense = expense[
                        ['Expense forecast (AED.)', 'Unnamed: 3', 'Unnamed: 24', 'Unnamed: 27', 'Unnamed: 30',
                         'Unnamed: 33',
                         'Unnamed: 36', 'Unnamed: 39', 'Unnamed: 42', 'Unnamed: 45', 'Unnamed: 48', 'Unnamed: 51',
                         'Unnamed: 54', 'Unnamed: 57']]
                    expense = expense.drop([0, 1])
                    expense['Expense forecast (AED.)'] = expense['Expense forecast (AED.)'].str.strip()
                    expense = expense.dropna(subset=['Expense forecast (AED.)'])
                    expense = expense.rename(
                        columns={'Expense forecast (AED.)': 'Sub_Head', 'Unnamed: 3': 'FTY', 'Unnamed: 24': 'Apr-25',
                                 'Unnamed: 27': 'May-25', 'Unnamed: 30': 'Jun-25',
                                 'Unnamed: 33': 'Jul-25', 'Unnamed: 36': 'Aug-25',
                                 'Unnamed: 39': 'Sep-25', 'Unnamed: 42': 'Oct-25',
                                 'Unnamed: 45': 'Nov-25', 'Unnamed: 48': 'Dec-25',
                                 'Unnamed: 51': 'Jan-26', 'Unnamed: 54': 'Feb-26',
                                 'Unnamed: 57': 'Mar-26'})

                expense = expense.reset_index()
                expense = expense.fillna(0)
                expense['Hotel Code'] = hotel
                expense['Sub_Head'] = expense['Sub_Head'].str.strip()
                expense = expense.replace({'TOTAL Renovation': 'Extraordinary Expenses'})

                # expected month + FTY columns
                month_cols = ['FTY', 'Apr-25', 'May-25', 'Jun-25', 'Jul-25', 'Aug-25', 'Sep-25', 'Oct-25', 'Nov-25',
                              'Dec-25', 'Jan-26', 'Feb-26', 'Mar-26']

                row = expense[expense["Sub_Head"] == "Festival Expenses"].to_dict("records")

                # ensure numeric for all target columns
                for col in month_cols:
                    if col in expense.columns:
                        expense[col] = pd.to_numeric(expense[col], errors='coerce').fillna(0)

                # group and force reindex to keep all columns
                expense = (
                    expense.groupby(['Sub_Head', 'Hotel Code'])[month_cols]
                    .sum(min_count=1)  # keeps 0 cols if all NaN
                    .reindex(columns=month_cols, fill_value=0)  # ensure all expected cols exist
                    .reset_index()
                )

                # Income Budget
                print("reading for income sheet")
                df = pd.read_excel(file_path, sheet_name='Income', header=1)
                AOP = pd.concat([AOP, df])
                AOP = AOP.drop(AOP.columns[[0, 1]], axis=1)
                AOP = AOP.drop([0, 1])
                AOP = AOP.dropna(how='all')
                AOP = AOP.reset_index(drop=True)
                AOP = AOP.drop(columns=['FTY', 'Unnamed: 5', 'H1',
                                        'Unnamed: 8', 'Unnamed: 9', 'H2', 'Unnamed: 11', 'Unnamed: 12', 'Q1',
                                        'Unnamed: 14', 'Unnamed: 15', 'Q2', 'Unnamed: 17', 'Unnamed: 18', 'Q3',
                                        'Unnamed: 20', 'Unnamed: 21', 'Q4', 'Unnamed: 23', 'Unnamed: 24', 'Apr',
                                        'Unnamed: 26', 'May', 'Unnamed: 29', 'Jun', 'Unnamed: 32', 'Jul', 'Unnamed: 35',
                                        'Aug', 'Unnamed: 38', 'Sep', 'Unnamed: 41', 'Oct', 'Unnamed: 44', 'Nov',
                                        'Unnamed: 47', 'Dec', 'Unnamed: 50', 'Jan', 'Unnamed: 53', 'Feb',
                                        'Unnamed: 56', 'Mar', 'Unnamed: 59', ])

                AOP = AOP.rename(columns={'Unnamed: 6': 'FTY', 'Unnamed: 27': 'Apr-25', 'Unnamed: 30': 'May-25',
                                          'Unnamed: 33': 'Jun-25',
                                          'Unnamed: 36': 'Jul-25', 'Unnamed: 39': 'Aug-25', 'Unnamed: 42': 'Sep-25',
                                          'Unnamed: 45': 'Oct-25', 'Unnamed: 48': 'Nov-25', 'Unnamed: 51': 'Dec-25',
                                          'Unnamed: 54': 'Jan-26', 'Unnamed: 57': 'Feb-26', 'Unnamed: 60': 'Mar-26'})

                df1 = AOP[AOP['Unnamed: 3'].isnull()]
                df1 = df1.dropna(how='all')
                df1 = df1.drop(columns=['Unnamed: 3'])
                df1 = df1.rename(columns={'Unnamed: 2': 'Sub_Head'})
                df1 = df1.dropna(how='all',
                                 subset=['Apr-25', 'May-25', 'Jun-25', 'Jul-25', 'Aug-25', 'Sep-25', 'Oct-25', 'Nov-25',
                                         'Dec-25', 'Jan-26', 'Feb-26', 'Mar-26'])
                df1 = df1.iloc[:, :14]

                df2 = AOP[AOP['Unnamed: 3'].notnull()]
                df2 = df2.dropna(how='all')
                df2 = df2.drop(columns=['Unnamed: 2'])
                df2 = df2.rename(columns={'Unnamed: 3': 'Sub_Head'})
                df2 = df2.dropna(how='all',
                                 subset=['Apr-25', 'May-25', 'Jun-25', 'Jul-25', 'Aug-25', 'Sep-25', 'Oct-25', 'Nov-25',
                                         'Dec-25', 'Jan-26', 'Feb-26', 'Mar-26'])
                df2 = df2.iloc[:, :14]

                result = pd.concat([df1, df2], ignore_index=True)
                result = result.dropna(how='all', subset=['Sub_Head'])

                df = result[result['Sub_Head'].str.contains(
                    'Banquets |Banquets|Banquet Miscellaneous|The Crest|Banquet Miscellaneous|Business Center|Refresh|refresh|Speciality|Swimming Pool/ AU Cafe|RON / ARIVA - The Bar|Swimming Pool - Bar|MINI BAR / Kebab Theatre|AU - Bar|Refresh|Pool BAR & Grill|The Riverfront Grill|Pomelo Bar|Pomelo / BAR|AU Café|The Terrace|Tipsy|Lagoon Bar|Gross ARR|Wysteria|Orchard|Pool BAR & Grill|Pool Bar and Grill|RON|Slounge/BAR|Slounge / BAR|Slounge/Bar|Slounge / Bar|RON / ARIVA - The Bar|RON/ARIVA-The Bar|RON|Swimming Pool - Bar|MINI BAR / Kebab Theatre|MINI BAR/Kebab Theatre|MINI BAR|Mini Bar|Kebab Theatre|Tea Lounge|Unlock/BAR|Unlock Bar|Banquet|In Room Dining|Slounge|Revel Café|River Front Grill|Skinners|Bitters|Crest Food|Pug Lounge|Amilo|Ariva|Mirasa|Swimming Pool Food|Other Food Café/Bar|Roast & Red|Unlock Bar|Recreation Bar|SPA|Internet|Refresh Recreation Activities|Service Charge|Banquet|Business Center|Banquet Miscellaneous|Communication|Gross Room Income|Net Room Income|Occupancy|RPD|Spa/Fresco|In Room Dining|IRD|IRD+Minibar|IRDMinibar')]
                df = df.reset_index(drop=True)

                # Read and process data for each hotel
                non_inc = pd.DataFrame()
                # for hotel_code, file_path in hotel_data:
                print("reading for fnb sheet")
                df_inc = pd.read_excel(file_path, sheet_name='F&B and Other Income')
                hotel = hotel_code
                non_inc = pd.concat([non_inc, df_inc], ignore_index=True)
                # print("columns in non_inc are - ", non_inc.columns)
                non_inc = non_inc.drop(columns=non_inc.columns[non_inc.columns.str.contains('FTY|FY')])
                non_inc = non_inc.drop(columns=['Unnamed: 2', 'H1', 'Unnamed: 5', 'Unnamed: 6', 'H2', 'Unnamed: 8',
                                                'Unnamed: 9', 'Q1', 'Unnamed: 11', 'Unnamed: 12', 'Q2', 'Unnamed: 14',
                                                'Unnamed: 15', 'Q3', 'Unnamed: 17', 'Unnamed: 18', 'Q4', 'Unnamed: 20',
                                                'Unnamed: 21', 'April', 'Unnamed: 23', 'May', 'Unnamed: 26', 'June',
                                                'Unnamed: 29', 'July', 'Unnamed: 32', 'August', 'Unnamed: 35',
                                                'September',
                                                'Unnamed: 38', 'October', 'Unnamed: 41', 'November', 'Unnamed: 44',
                                                'December', 'Unnamed: 47', 'January', 'Unnamed: 50', 'February',
                                                'Unnamed: 53', 'March', 'Unnamed: 56', ])

                non_inc = non_inc.rename(columns={non_inc.columns[0]: 'Sub_Head'})

                non_inc = non_inc.rename(columns={'Sub_Head': 'Sub_Head', 'Unnamed: 3': 'FTY', 'Unnamed: 24': 'Apr-25',
                                                  'Unnamed: 27': 'May-25', 'Unnamed: 30': 'Jun-25',
                                                  'Unnamed: 33': 'Jul-25', 'Unnamed: 36': 'Aug-25',
                                                  'Unnamed: 39': 'Sep-25', 'Unnamed: 42': 'Oct-25',
                                                  'Unnamed: 45': 'Nov-25', 'Unnamed: 48': 'Dec-25',
                                                  'Unnamed: 51': 'Jan-26', 'Unnamed: 54': 'Feb-26',
                                                  'Unnamed: 57': 'Mar-26'})

                non_inc = non_inc.iloc[:, :14]

                index_number = non_inc.loc[non_inc['Sub_Head'] == 'Total Non Inclusive Sale'].index[0]

                start_index = index_number + 1
                end_index = index_number + 6

                rows_slice = non_inc.iloc[start_index:end_index]
                rows_slice = rows_slice.reset_index(drop=True)
                rows_slice = rows_slice.replace('F&B ', ' # FnB')
                rows_slice.iloc[:, 1:] = rows_slice.iloc[:, 1:] / 1000

                index_number2 = non_inc.loc[(non_inc['Sub_Head'] == 'CITRUS ') |
                                            (non_inc['Sub_Head'] == 'CITRUS') |
                                            (non_inc['Sub_Head'] == 'CITRUS / MIRASA') |
                                            (non_inc['Sub_Head'] == 'Mirasa') |
                                            (non_inc['Sub_Head'] == 'Clever Café') |
                                            (non_inc['Sub_Head'] == 'Keys Café ') |
                                            (non_inc['Sub_Head'] == 'Keys Café')].index[0]

                start_index2 = index_number2 + 3
                end_index2 = index_number2 + 4

                rows_slice2 = non_inc.iloc[start_index2:end_index2]
                rows_slice2 = rows_slice2.reset_index(drop=True)

                if non_inc.iloc[index_number2]['Sub_Head'] == 'CITRUS ':
                    rows_slice2 = rows_slice2.replace({'Non Inclusive Sale': 'Citrus'})
                elif non_inc.iloc[index_number2]['Sub_Head'] == 'CITRUS':
                    rows_slice2 = rows_slice2.replace({'Non Inclusive Sale': 'Citrus'})
                elif non_inc.iloc[index_number2]['Sub_Head'] == 'Clever Café':
                    rows_slice2 = rows_slice2.replace({'Non Inclusive Sale': 'Clever Café'})
                elif non_inc.iloc[index_number2]['Sub_Head'] == 'CITRUS / MIRASA':
                    rows_slice2 = rows_slice2.replace({'Non Inclusive Sale': 'Mirasa'})
                elif non_inc.iloc[index_number2]['Sub_Head'] == 'Mirasa':
                    rows_slice2 = rows_slice2.replace({'Non Inclusive Sale': 'Mirasa'})
                elif non_inc.iloc[index_number]['Sub_Head'] == 'Keys Café ':
                    rows_slice2 = rows_slice2.replace({'Non Inclusive Sale': 'Keys Café'})
                else:
                    rows_slice2 = rows_slice2.replace({'Non Inclusive Sale': 'Keys Café'})
                rows_slice2.iloc[:, 1:] = rows_slice2.iloc[:, 1:] / 1000

                # Inclusive Budget
                print("in inclusive budget")
                index_number3 = non_inc.loc[non_inc['Sub_Head'] == 'Total Inclusive Sale'].index[0]

                start_index3 = index_number3 + 1
                end_index3 = index_number3 + 6

                rows_slice3 = non_inc.iloc[start_index3:end_index3]
                rows_slice3 = rows_slice3.reset_index(drop=True)
                rows_slice3 = rows_slice3.replace('F&B ', 'Inclusives F&B')
                rows_slice3 = rows_slice3.replace('Laundry', 'Inclusives Laundry')
                rows_slice3 = rows_slice3.replace('Transport ', 'Inclusives Trans')
                rows_slice3 = rows_slice3.replace('Car hire', 'Inclusives Car')
                rows_slice3 = rows_slice3.replace('Internet ', 'Inclusives In')
                rows_slice3.iloc[:, 1:] = rows_slice3.iloc[:, 1:] / 1000

                non_inclusive = pd.concat([df, rows_slice], ignore_index=True)
                non_inclusive = pd.concat([non_inclusive, rows_slice2], ignore_index=True)
                non_inclusive = pd.concat([non_inclusive, rows_slice3], ignore_index=True)
                non_inclusive = non_inclusive.reset_index(drop=True)

                # banquet_list = 'Banquet'
                # banquet_df = non_inclusive[non_inclusive['Sub_Head'].str.contains(banquet_list)]
                # banquet = pd.DataFrame({'Sub_Head': ['Banquet'],**{col: [banquet_df[col].sum()] for col in banquet_df.columns if col != 'Sub_Head'}})

                internet_list = 'Internet|Internet/Telephone|Telephone'
                Internet_df = non_inclusive[non_inclusive['Sub_Head'].str.contains(internet_list)]
                Internet = pd.DataFrame({'Sub_Head': ['Internet'],
                                         **{col: [Internet_df[col].sum()] for col in Internet_df.columns if
                                            col != 'Sub_Head'}})

                Internet_list2 = 'Inclusives In'
                Internet_df2 = non_inclusive[non_inclusive['Sub_Head'].str.contains(Internet_list2)]
                Internet2 = pd.DataFrame({'Sub_Head': ['Internet Inclusives'],
                                          **{col: [Internet_df2[col].sum()] for col in Internet_df2.columns if
                                             col != 'Sub_Head'}})

                # others_list = 'Banquet Miscellaneous|Business Center|Refresh|refresh|SPA|Spa|Spa/Fresco|SPA/Fresco|spa / Fresco|spa / fresco|SPA / Fresco|spa|Internet|Refresh Recreation Activities|Miscelleneous|Business Center'
                # others_df = non_inclusive[non_inclusive['Sub_Head'].str.contains(others_list)]
                # Others = pd.DataFrame({'Sub_Head': [' # Others'], **{col: [others_df[col].sum()] for col in others_df.columns if col != 'Sub_Head'}})

                Laundry_list = 'Laundry'
                Laundry_df = non_inclusive[non_inclusive['Sub_Head'].str.contains(Laundry_list)]
                Laundry = pd.DataFrame({'Sub_Head': [' # Laundry'],
                                        **{col: [Laundry_df[col].sum()] for col in Laundry_df.columns if
                                           col != 'Sub_Head'}})

                Transport_list = 'Transport|Car hire'
                Transport_df = non_inclusive[non_inclusive['Sub_Head'].str.contains(Transport_list)]
                Transport = pd.DataFrame({'Sub_Head': [' # Transport'],
                                          **{col: [Transport_df[col].sum()] for col in Transport_df.columns if
                                             col != 'Sub_Head'}})

                Transport_list2 = 'Transport|Car hire'
                Transport_df2 = non_inclusive[non_inclusive['Sub_Head'].str.contains(Transport_list2)]
                Transport2 = pd.DataFrame({'Sub_Head': ['Transport'],
                                           **{col: [Transport_df2[col].sum()] for col in Transport_df.columns if
                                              col != 'Sub_Head'}})

                Transport_list3 = 'Inclusives Trans|Inclusives Car'
                Transport_df3 = non_inclusive[non_inclusive['Sub_Head'].str.contains(Transport_list3)]
                Transport3 = pd.DataFrame({'Sub_Head': ['Transport Inclusives'],
                                           **{col: [Transport_df3[col].sum()] for col in Transport_df3.columns if
                                              col != 'Sub_Head'}})

                Service_list = 'Service Charge'
                Service_df = non_inclusive[non_inclusive['Sub_Head'].str.contains(Service_list)]
                Service = pd.DataFrame({'Sub_Head': [' # Service Charge'],
                                        **{col: [Service_df[col].sum()] for col in Service_df.columns if
                                           col != 'Sub_Head'}})

                FnB_list = ' # FnB|Banquet Miscellaneous'
                FnB_df = non_inclusive[non_inclusive['Sub_Head'].str.contains(FnB_list)]
                FnB = pd.DataFrame({'Sub_Head': [' # FnB'],
                                    **{col: [FnB_df[col].sum()] for col in FnB_df.columns if col != 'Sub_Head'}})

                Ancilliary_revenue = pd.concat([Transport, Laundry], ignore_index=True)
                Ancilliary_revenue = pd.concat([Ancilliary_revenue, Service], ignore_index=True)
                Ancilliary_revenue = pd.concat([Ancilliary_revenue, FnB], ignore_index=True)
                Ancilliary = pd.DataFrame({'Sub_Head': ['Ancillary Revenue'],
                                           **{col: [Ancilliary_revenue[col].sum()] for col in Ancilliary_revenue.columns
                                              if col != 'Sub_Head'}})

                Room_list = 'Gross Room Income'
                Room_df = non_inclusive[non_inclusive['Sub_Head'].str.contains(Room_list)]
                Room = pd.DataFrame({'Sub_Head': ['Room Revenue'],
                                     **{col: [Room_df[col].sum()] for col in Room_df.columns if col != 'Sub_Head'}})

                Net_Room_list = 'Net Room Income'
                Net_Room_df = non_inclusive[non_inclusive['Sub_Head'].str.contains(Net_Room_list)]
                Net_Room = pd.DataFrame({'Sub_Head': ['Net Room Revenue'],
                                         **{col: [Net_Room_df[col].sum()] for col in Net_Room_df.columns if
                                            col != 'Sub_Head'}})

                # In Room List
                print("in In Room List")
                InRoom_list = 'IRD|IRD+Minibar|In Room Dining|IRDMinibar'
                InRoom_df = non_inclusive[non_inclusive['Sub_Head'].str.contains(InRoom_list)]
                InRoom = pd.DataFrame({'Sub_Head': ['In Room Dining'],
                                       **{col: [InRoom_df[col].sum()] for col in InRoom_df.columns if
                                          col != 'Sub_Head'}})

                # Total Revenue
                Total_Revenue = pd.concat([Room, Ancilliary], ignore_index=True)
                Total = pd.DataFrame({'Sub_Head': ['Total Revenue'],
                                      **{col: [Total_Revenue[col].sum()] for col in Total_Revenue.columns if
                                         col != 'Sub_Head'}})

                # Create a dictionary with month names and corresponding days
                data = {
                    'Month': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'FTY'],
                    'Days': [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 365]
                }

                # Create DataFrame for Gros room Income
                days = pd.DataFrame(data)

                # Net Room Income
                Net_Room_list = 'Net Room Income'
                Net_df = non_inclusive[non_inclusive['Sub_Head'].str.contains(Net_Room_list)]

                Net_Room = Net_df.T
                Net_Room = Net_Room.reset_index()

                Net_Room.columns = Net_Room.iloc[0]
                # Dropping the first row
                Net_Room = Net_Room.drop(0)

                Net_Room = Net_Room.reset_index()

                # Gross Room Income per Day
                Gross_Room_list = 'Gross Room Income'
                Gross_df = non_inclusive[non_inclusive['Sub_Head'].str.contains(Gross_Room_list)]

                Gross = Gross_df.T
                Gross = Gross.reset_index()

                Gross.columns = Gross.iloc[0]
                # Dropping the first row
                Gross = Gross.drop(0)

                Gross = Gross.reset_index()
                Gross_b = Gross.merge(days, left_on='Sub_Head', right_on='Month', how='left')
                Gross_b['Gross Room Income/day'] = Gross_b['Gross Room Income'] / Gross_b['Days']
                Gross_b['RRPD'] = ((Gross_b['Gross Room Income'] / Gross_b['Days']) * 1000)

                Gross_day = Gross_b.copy()
                Gross_day = Gross_day.drop(columns=['Month', 'Days', 'index', 'Gross Room Income', 'RRPD'])
                Gross_day = Gross_day.T
                Gross_day.columns = Gross_day.iloc[0]

                # Gross_day = Gross_day.reset_index()
                # Dropping the first row
                Gross_day = Gross_day.drop('Sub_Head')
                Gross_day = Gross_day.reset_index()
                Gross_day = Gross_day.rename(columns={'index': 'Sub_Head'})

                # RRPD
                RRPD = Gross_b.copy()
                RRPD = RRPD.drop(columns=['Month', 'Days', 'index', 'Gross Room Income', 'Gross Room Income/day'])
                RRPD = RRPD.T
                RRPD.columns = RRPD.iloc[0]

                # Gross_day = Gross_day.reset_index()
                # Dropping the first row
                RRPD = RRPD.drop('Sub_Head')
                RRPD = RRPD.reset_index()
                RRPD = RRPD.rename(columns={'index': 'Sub_Head'})

                # Room Inclusives
                print("in InRoom Inclusivest")
                Gross_c = Gross.merge(Net_Room, on='Sub_Head', how='inner')
                Gross_c['Room Income Inclusives'] = Gross_c['Net Room Income'] - Gross_c['Gross Room Income']
                Net_day = Gross_c.copy()
                Net_day = Net_day.drop(columns=['index_x', 'index_y', 'Gross Room Income', 'Net Room Income'])
                Net_day = Net_day.T
                Net_day.columns = Net_day.iloc[0]
                Net_day = Net_day.drop('Sub_Head')
                Net_day = Net_day.reset_index()
                Net_day = Net_day.rename(columns={'index': 'Sub_Head'})
                Net_day = Net_day.reset_index(drop=True)
                Net_day = Net_day.rename(columns={Net_day.columns[0]: 'Sub_Head'})

                AOP_list = 'Banquets |Banquets|Banquet|Banquet Miscellaneous|The Crest|Business Center|Inclusives Laundry|Inclusives In|Inclusives F&B|Speciality|Swimming Pool/ AU Cafe|RON / ARIVA - The Bar|Swimming Pool - Bar|MINI BAR / Kebab Theatre|AU - Bar|Refresh|Pool BAR & Grill|The Riverfront Grill|Refresh|Kebab Theatre|Tea Lounge|Citrus|Clever Café|Keys Café|Mirasa|Pomelo Bar|Pomelo / BAR|AU Café|The Terrace|Tipsy|Lagoon Bar|Wysteria|Orchard|RON|Unlock/BAR|Unlock Bar|RON / ARIVA - The Bar|MINI BAR / Kebab Theatre|Swimming Pool - Bar|RON / ARIVA - The Bar|Slounge|Revel Café|River Front Grill|Pool and Bar Grill|Skinners|Bitters|Crest Food|Pug Lounge|Amilo|Ariva|Swimming Pool Food|Other Food Café/Bar|Roast & Red|Unlock Bar|Recreation Bar|SPA|Laundry|Occupancy|Gross ARR|RPD|Spa/Fresco|Internet'
                income = non_inclusive[non_inclusive['Sub_Head'].str.contains(AOP_list)]
                # income = pd.concat([income,banquet], ignore_index=True)
                # income = pd.concat([income,Others], ignore_index=True)
                income = pd.concat([income, FnB], ignore_index=True)
                income = pd.concat([income, Laundry], ignore_index=True)
                # income = pd.concat([income,Transport], ignore_index=True)
                income = pd.concat([income, Transport2], ignore_index=True)
                income = pd.concat([income, Transport3], ignore_index=True)
                income = pd.concat([income, Service], ignore_index=True)
                income = pd.concat([income, Internet], ignore_index=True)
                income = pd.concat([income, Internet2], ignore_index=True)
                income = pd.concat([income, Ancilliary], ignore_index=True)
                income = pd.concat([income, Room], ignore_index=True)
                income = pd.concat([income, Total], ignore_index=True)
                income = pd.concat([income, InRoom], ignore_index=True)
                income = pd.concat([income, Gross_day], ignore_index=True)
                income = pd.concat([income, RRPD], ignore_index=True)
                income = pd.concat([income, Net_day], ignore_index=True)

                income = income.replace({
                    'Unlock/BAR': 'Unlock Bar',
                    'Spa/Fresco': 'SPA',
                    'Slounge/BAR': 'Slounge',
                    'Slounge / BAR': 'Slounge',
                    'Slounge/Bar': 'Slounge',
                    'Slounge / Bar': 'Slounge',
                    'RON / ARIVA - The Bar': 'Ariva',
                    'Keys CafÃ© ': 'Keys Cafe',
                    'Keys Café': 'Keys Cafe',
                    'MINI BAR / Kebab Theatre': 'Mini Bar',
                    'CITRUS  / MIRASA': 'Citrus',
                    'Swimming Pool - Bar': 'Swimming Pool Food',
                    'Unlock/Bar': 'Unlock Bar',
                    'The Crest': 'Crest Food',
                    'Slounge Bar': 'Slounge',
                    'The Riverfront Grill': 'Riverfront Grill',
                    'Business Center': 'Business Center Income',
                    'Crest Food': 'The Crest',
                    'The Crest': 'The Crest',
                    'Banquets': 'Banquet',
                    'Banquets ': 'Banquet',
                    'Inclusives F&B': 'Food & Beverage Inclusives',
                    'Inclusives Laundry': 'Laundry Inclusives',
                    'Inclusives In': 'Internet Inclusives'
                })
                income['hotel_code'] = hotel
                income = income.fillna(0)

                # final = final.merge(mapping, on = 'hotel_code', how = 'left')
                income['Sub_Head'] = income['Sub_Head'].str.strip()
                columns_to_multiply = ['FTY', 'Apr-25', 'May-25', 'Jun-25', 'Jul-25', 'Aug-25', 'Sep-25', 'Oct-25',
                                       'Nov-25', 'Dec-25', 'Jan-26', 'Feb-26', 'Mar-26']
                income[columns_to_multiply] = income[columns_to_multiply] * 1000

                # EXPENSE GL CODES
                print("EXPENSE GL CODES:" + expense_file_path)
                expense_mapping = pd.read_excel(expense_file_path)
                expense_mapping['GL Code'] = expense_mapping['GL Code'].str.strip()
                expense_mapping['GL Name'] = expense_mapping['GL Name'].str.strip()
                expense_mapping['GL Code & Name'] = expense_mapping['GL Code'] + ' ' + expense_mapping['GL Name']
                expense['Sub_Head'] = expense['Sub_Head'].str.lower()
                expense['Sub_Head'] = expense['Sub_Head'].str.strip()
                expense_mapping['Sub_Head'] = expense_mapping['Sub_Head'].str.lower()
                expense_mapping['Sub_Head'] = expense_mapping['Sub_Head'].str.strip()
                expense = expense.drop_duplicates(subset=['Sub_Head', 'Hotel Code'])
                expense_final = pd.merge(expense_mapping, expense, on=['Sub_Head'], how='left')
                expense_final = expense_final.dropna(subset=['Sub_Head'])
                expense_final = expense_final.drop_duplicates(subset=['Sub_Head', 'Hotel Code'])
                expense_final = expense_final.groupby(['GL Code', 'GL Name', 'GL Code & Name'])[
                    ['FTY', 'Apr-25', 'May-25', 'Jun-25', 'Jul-25', 'Aug-25', 'Sep-25', 'Oct-25', 'Nov-25', 'Dec-25',
                     'Jan-26', 'Feb-26', 'Mar-26']].sum().reset_index()
                expense_final['budget'] = 'Expense'

                # INCOME GL CODES
                print("INCOME GL CODES")
                income_mapping = pd.read_excel(income_file_path, sheet_name='Budget Income Mapping')
                income_mapping['GL Code'] = income_mapping['GL Code'].str.strip()
                income_mapping['GL Name'] = income_mapping['GL Name'].str.strip()
                income = income.rename(columns={'hotel_code': 'Hotel Code'})
                income_mapping = income_mapping.drop(
                    columns=['Category of Revenue as per Reporting Pack', 'Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6',
                             'Unnamed: 7',
                             'Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12',
                             'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15'])
                income_mapping['GL Code & Name'] = income_mapping['GL Code'] + ' ' + income_mapping['GL Name']
                income['Sub_Head'] = income['Sub_Head'].str.lower()
                income['Sub_Head'] = income['Sub_Head'].str.strip()
                income_mapping['Sub_Head'] = income_mapping['Sub_Head'].str.lower()
                income_mapping['Sub_Head'] = income_mapping['Sub_Head'].str.strip()

                income_final = pd.merge(income_mapping, income, on=['Sub_Head'], how='left')
                income_final.dropna(subset=['Sub_Head'])
                income_final = income_final.drop_duplicates(subset=['Sub_Head', 'Hotel Code'])
                income_final = income_final.drop(columns=['Sub_Head', 'Hotel Code'])
                income_final['budget'] = 'Income'

                # COMBINING INCOME AND EXPENSE BUDGETS
                print("COMBINING INCOME AND EXPENSE BUDGETS")
                compiled_budget = pd.concat([income_final, expense_final], ignore_index=True)
                compiled_budget = compiled_budget.dropna()

                fsli_budget = pd.merge(fsli_mapping, compiled_budget.drop(['GL Name', 'GL Code & Name'], axis=1), on=['GL Code'], how='right')

                # Drop the unnecessary columns
                fsli_budget = fsli_budget.drop(columns=['Department Profitability', 'FTY', 'budget'])

                try:
                    # Last Budget
                    print("reading last year budget")
                    last_bud = pd.read_excel(f'{gl_level_budget_path}/{code}_gl_level_budget.xlsx', header=3)

                    # keep a backup to this directory
                    complete_backup_path = back_up_gl_level_budget_path + f'/{code}_gl_level_budget.xlsx'
                    pd.read_excel(f'{gl_level_budget_path}/{code}_gl_level_budget.xlsx').to_excel(complete_backup_path, index=False, sheet_name=f'{code}')
                    print(f'Back up created at : {complete_backup_path}')

                    last_bud = last_bud.drop(
                        columns={'Apr-25', 'May-25', 'Jun-25', 'Jul-25', 'Aug-25', 'Sep-25', 'Oct-25',
                                 'Nov-25', 'Dec-25', 'Jan-26', 'Feb-26', 'Mar-26'})
                    resulted_bud = (last_bud.drop(['GL Name', 'GL Code & Name', 'FSLI Mapping', 'FS Category', 'FS Mapping'], axis=1).
                                    merge(fsli_budget, on=[ 'GL Code'], how='outer'))

                except FileNotFoundError as foe:
                    print(foe)
                    resulted_bud = fsli_budget.copy()
                    cols_to_add = ['Apr-24', 'May-24', 'Jun-24', 'Jul-24', 'Aug-24', 'Sep-24', 'Oct-24', 'Nov-24',
                                   'Dec-24', 'Jan-25', 'Feb-25', 'Mar-25']
                    resulted_bud[cols_to_add] = 0

                except Exception as e:
                    print(f"An error occurred: {e}")

                # Select the required columns for the resulting DataFrame
                resulted_budget_columns = ['FS Mapping', 'FS Category', 'FSLI Mapping', 'GL Code & Name',
                                           'GL Code', 'GL Name', 'Apr-24', 'May-24', 'Jun-24', 'Jul-24', 'Aug-24',
                                           'Sep-24', 'Oct-24', 'Nov-24', 'Dec-24', 'Jan-25', 'Feb-25', 'Mar-25',
                                           'Apr-25', 'May-25', 'Jun-25', 'Jul-25', 'Aug-25', 'Sep-25', 'Oct-25',
                                           'Nov-25', 'Dec-25', 'Jan-26', 'Feb-26', 'Mar-26']

                resulted_budget = resulted_bud[resulted_budget_columns]
                columns = ['Apr-24', 'May-24', 'Jun-24', 'Jul-24', 'Aug-24',
                           'Sep-24', 'Oct-24', 'Nov-24', 'Dec-24', 'Jan-25', 'Feb-25', 'Mar-25',
                           'Apr-25', 'May-25', 'Jun-25', 'Jul-25', 'Aug-25', 'Sep-25', 'Oct-25',
                           'Nov-25', 'Dec-25', 'Jan-26', 'Feb-26', 'Mar-26']
                resulted_budget[columns] = resulted_budget[columns].fillna(0)
                # print("updating parquet file location")
                # resulted_budget.to_parquet(f'{output_parquet_bucket}/{code}_budget.parquet')

                # Create the Excel file name for the final_tb
                excel_file_name = f'/{code}_gl_level_budget.xlsx'
                resulted_budget.to_excel(f'{gl_level_budget_path}{excel_file_name}', index=False, sheet_name=f'{code}')

        except Exception as e:
            print(f"Error found while processing hotel_code: {code}. Error: ", e)
            error_list.append(code)

    if len(error_list) > 0:
        print("Error found while processing the below hotels.")
        for item in error_list:
            print(item)
        send_email_with_attachments(notify_email, None, None, None, None,
                                    f"Processing of AOP Budget failed for hotel codes: {', '.join(error_list)}", "ERROR: AOP Pnl JOB")
    else:
        print("Processing completed without any errors.")


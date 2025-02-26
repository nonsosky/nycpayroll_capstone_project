
#Import Necessary Libraries
import pandas as pd
import os
import glob
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import logging


load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")
DB_PASS = os.getenv("DB_PASSWORD")
DB_PASSWORD = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")

############################################################################# EXTRACTION ###############################################################################################

def extract_data(**kwargs):
    try:
        
        #Load Master Data
        employee_df = pd.read_csv('dataset/raw/EmpMaster.csv')
        agency_df = pd.read_csv('dataset/raw/AgencyMaster.csv')
        jobtitle_df = pd.read_csv('dataset/raw/TitleMaster.csv')

        # Define directory containing payroll CSV files
        payroll_dir = "dataset/payroll_data"

        # Find all payroll CSV files dynamically
        payroll_files = glob.glob(os.path.join(payroll_dir, "nycpayroll_*.csv"))

        # Check if any files were found
        if not payroll_files:
            raise ValueError("No payroll files found in the directory!")

        # Function to Load and Merge Payroll Data
        def load_payroll_data(files):
            dataframes = [pd.read_csv(file) for file in files]  # Read all CSV files into Pandas DataFrames
            merged_df = pd.concat(dataframes, ignore_index=True)  # Concatenate all dataframes
            merged_df.drop_duplicates(subset=["EmployeeID", "FiscalYear"], inplace=True)  # Drop duplicate rows
            return merged_df

        # Load and process payroll data
        payroll_df = load_payroll_data(payroll_files)

        # Push file paths to XCom
        ti = kwargs['ti']
        ti.xcom_push(key='payroll_df', value=payroll_df)
        ti.xcom_push(key='employee_df', value=employee_df)
        ti.xcom_push(key='agency_df', value=agency_df)
        ti.xcom_push(key='jobtitle_df', value=jobtitle_df)

        logging.info("Data extraction successful.")

    except Exception as e:
        logging.error(f"Error extracting data: {e}")
        raise

############################################################################# TRANSFORMATION ###############################################################################################

def transform_data(**kwargs):
    ti = kwargs['ti']
    
    # Pull entire data dictionary
    payroll_df = ti.xcom_pull(key='payroll_df', task_ids='Data_Extraction')
    employee_df = ti.xcom_pull(key='employee_df', task_ids='Data_Extraction')
    agency_df = ti.xcom_pull(key='agency_df', task_ids='Data_Extraction')
    jobtitle_df = ti.xcom_pull(key='jobtitle_df', task_ids='Data_Extraction')
    
    try:

        # Fill null values with defaults
        for col_name, dtype in payroll_df.dtypes.items():
            if dtype == "object":  # Strings
                payroll_df = payroll_df.assign(**{col_name: payroll_df[col_name].fillna("Unknown")})
            elif dtype in ["float64", "float32"]:  # Floats
                payroll_df = payroll_df.assign(**{col_name: payroll_df[col_name].fillna(0.0)})
            elif dtype in ["int64", "int32"]:  # Integers
                payroll_df = payroll_df.assign(**{col_name: payroll_df[col_name].fillna(0)})


        # Merge all data together
        merged_data = payroll_df \
            .merge(employee_df, left_on=["EmployeeID", "LastName", "FirstName"], right_on=["EmployeeID", "LastName", "FirstName"], how="left") \
            .merge(agency_df, left_on=["AgencyID", "AgencyName"], right_on=["AgencyID", "AgencyName"], how="left") \
            .merge(jobtitle_df, left_on=["TitleCode", "TitleDescription"], right_on=["TitleCode", "TitleDescription"], how="left")

        # Convert AgencyStartDate from object to datetime
        merged_data["AgencyStartDate"] = pd.to_datetime(merged_data["AgencyStartDate"], format="%m/%d/%Y", errors="coerce").dt.date

        # Create Employee Dimension Table
        employee_dim = merged_data[['EmployeeID', 'LastName', 'FirstName', 'WorkLocationBorough', 'LeaveStatusasofJune30']].copy().drop_duplicates(['EmployeeID']).reset_index(drop=True)
        employee_dim.head()

        #Create Agency Dimension Table
        agency_dim = merged_data[['AgencyID', 'AgencyName']].copy().drop_duplicates().reset_index(drop=True)
        agency_dim.head()

        #Create Job_Title Dimension Table
        jobtitle_dim = merged_data[['TitleCode', 'TitleDescription']].copy().drop_duplicates().reset_index(drop=True)
        jobtitle_dim.head()

        #Create Time Dimension Table
        time_dim = merged_data[['FiscalYear']].copy().drop_duplicates().reset_index(drop=True)
        time_dim['TimeID'] = range(1, len(time_dim) + 1)
        time_dim = time_dim[['TimeID', 'FiscalYear']]
        time_dim.head()

        # Create Payroll Fact Table using pandas
        payroll_fact_tbl = merged_data \
            .merge(employee_dim, on=['LastName', 'FirstName', 'LeaveStatusasofJune30', 'WorkLocationBorough'], how='left', suffixes=('', '_emp')) \
            .merge(agency_dim, on=['AgencyName'], how='left', suffixes=('', '_agency')) \
            .merge(jobtitle_dim, on=['TitleDescription'], how='left', suffixes=('', '_job')) \
            .merge(time_dim, on=['FiscalYear'], how='left', suffixes=('', '_time'))

        payroll_fact_tbl['PayrollID'] = range(1, len(payroll_fact_tbl) + 1)
        payroll_fact_tbl = payroll_fact_tbl[[
            'PayrollID', 'EmployeeID', 'AgencyID', 'TitleCode', 'TimeID',
            'PayrollNumber', 'BaseSalary', 'PayBasis', 'AgencyStartDate',
            'RegularHours', 'RegularGrossPaid', 'OTHours', 'TotalOTPaid', 'TotalOtherPay'
        ]]

        # Verify the output
        payroll_fact_tbl.head()

        # Save transformed data
        employee_dim.to_csv('dataset/cleaned_data/employee.csv', index=False)
        agency_dim.to_csv('dataset/cleaned_data/agency.csv', index=False)
        jobtitle_dim.to_csv('dataset/cleaned_data/jobtitle.csv', index=False)
        time_dim.to_csv('dataset/cleaned_data/time.csv', index=False)
        payroll_fact_tbl.to_csv('dataset/cleaned_data/payroll_fact_tbl.csv', index=False)

        # Push file paths to XCom
        ti.xcom_push(key='employee_dim', value=employee_dim)
        ti.xcom_push(key='agency_dim', value=agency_dim)
        ti.xcom_push(key='jobtitle_dim', value=jobtitle_dim)
        ti.xcom_push(key='time_dim', value=time_dim)
        ti.xcom_push(key='payroll_fact_tbl', value=payroll_fact_tbl)

        logging.info("Data Transformation Completed Successfully.")
    except Exception as e:
        logging.error(f"Data Transformation Failed: {e}")
        raise

############################################################################# LOADING ###############################################################################################

def load_data(**kwargs):
    try:
        ti = kwargs['ti']

        # Pull entire data dictionary
        payroll_fact_tbl = ti.xcom_pull(key='payroll_fact_tbl', task_ids='Data_Transformation')
        employee_dim = ti.xcom_pull(key='employee_dim', task_ids='Data_Transformation')
        agency_dim = ti.xcom_pull(key='agency_dim', task_ids='Data_Transformation')
        jobtitle_dim = ti.xcom_pull(key='jobtitle_dim', task_ids='Data_Transformation')
        time_dim = ti.xcom_pull(key='time_dim', task_ids='Data_Transformation')

        def get_db_connection():
            connection = psycopg2.connect(
                host =DB_HOST,
                database = DB_NAME,
                user = DB_USER,
                port = DB_PORT,
                password = DB_PASS
            )
            return connection

        #connect to our database
        conn = get_db_connection()

        # Create a function create tables
        def create_tables():
            conn = get_db_connection()
            cursor = conn.cursor()
            create_table_query = '''

                                CREATE SCHEMA IF NOT EXISTS nyc_payroll;

                                DROP TABLE IF EXISTS nyc_payroll.employee CASCADE;
                                DROP TABLE IF EXISTS nyc_payroll.agency CASCADE;
                                DROP TABLE IF EXISTS nyc_payroll.jobtitle CASCADE;
                                DROP TABLE IF EXISTS nyc_payroll.time CASCADE;
                                DROP TABLE IF EXISTS nyc_payroll.fact_table CASCADE;


                                CREATE TABLE IF NOT EXISTS nyc_payroll.employee(
                                    employeeid INT PRIMARY KEY,
                                    lastname VARCHAR(1000),
                                    firstname VARCHAR(1000),
                                    worklocationborough VARCHAR(1000),
                                    leavestatusasofjune30 VARCHAR(1000)
                                );

                                CREATE TABLE IF NOT EXISTS nyc_payroll.agency(
                                    agencyID INT PRIMARY KEY,
                                    agencyName VARCHAR(1000)
                                );

                                CREATE TABLE IF NOT EXISTS nyc_payroll.jobtitle(
                                    titleCode INT PRIMARY KEY,
                                    titleDescription VARCHAR(1000)
                                );

                                CREATE TABLE IF NOT EXISTS nyc_payroll.time(
                                    timeID INT PRIMARY KEY,
                                    fiscalYear INT
                                );

                                CREATE TABLE IF NOT EXISTS nyc_payroll.payroll_table(
                                    PayrollID INT PRIMARY KEY,
                                    EmployeeID INT,
                                    AgencyID INT,
                                    TitleCode INT,
                                    TimeID INT,
                                    PayrollNumber INT,
                                    BaseSalary DECIMAL(10,2),
                                    PayBasis VARCHAR(1000),
                                    AgencyStartDate DATE,
                                    RegularHours DECIMAL(10,2),
                                    RegularGrossPaid DECIMAL(10,2),
                                    OTHours DECIMAL(10,2),
                                    TotalOTPaid DECIMAL(10,2),
                                    TotalOtherPay DECIMAL(10,2),
                                    FOREIGN KEY (EmployeeID) REFERENCES nyc_payroll.employee(EmployeeID),
                                    FOREIGN KEY (AgencyId) REFERENCES nyc_payroll.agency(AgencyId),
                                    FOREIGN KEY (TitleCode) REFERENCES nyc_payroll.jobtitle(TitleCode),
                                    FOREIGN KEY (TimeID) REFERENCES nyc_payroll.time(TimeID)

                                );

                '''
            cursor.execute(create_table_query)
            conn.commit()
            cursor.close()
            conn.close()

        create_tables()

        engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}")

        try:
            payroll_fact_tbl.to_sql('payroll_table', con=engine, schema='nyc_payroll', if_exists='replace', index=False)
            agency_dim.to_sql('agency', con=engine, schema='nyc_payroll', if_exists='replace', index=False)
            jobtitle_dim.to_sql('jobtitle', con=engine, schema='nyc_payroll', if_exists='replace', index=False)
            time_dim.to_sql('time', con=engine, schema='nyc_payroll', if_exists='replace', index=False)
            employee_dim.to_sql('employee', con=engine, schema='nyc_payroll', if_exists='replace', index=False)
            logging.info("Table Created and Data loaded successfully to the database")
        except SQLAlchemyError as e:
            logging.error(f"Error while loading data into database:{e}")
            raise
    except Exception as e:
        logging.error(f"Load Data Task Failed: {e}")
        raise

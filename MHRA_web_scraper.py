
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import smtplib
from email.message import EmailMessage
import time
import socket
import tabula
import pandas as pd
import os
import pyodbc

# URL of the webpage to scrape
url = "https://www.gov.uk/government/publications/marketing-authorisations-granted-in-"
pi_url = "https://www.gov.uk/government/publications/parallel-import-licences-granted-in-"

this_year = str(datetime.now().year)

# Standard files 
full_url = url+this_year

directory_path = os.getenv('DIRECTORY_PATH')
save_directory_path = directory_path+this_year

# PI Files
pi_full_url = pi_url+this_year

directory_path_pi = os.getenv('PI_DIRECTORY_PATH')
pi_save_directory_path = directory_path_pi+this_year



# SQL queries ---------------------------------------------------------------
# SQL query broken into multiple lines for clarity
sql_insert_query = """
INSERT INTO [MHRA_licences] (
    [PL_number]
    ,[Grant_Date]
    ,[MA_holder]
    ,[Licenced_name]
    ,[Active_ingredient]
    ,[Quantity]
    ,[Units]
    ,[Legal_status]
    ,[Work_type]
    ,[Auth_status]
    ,[Territory]
    ,[type]
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
# keys in the db: PL_number, Licenced_name, Active_ingredient, Quantity
sql_select_query = """
SELECT * FROM [MHRA_licences_AN]
WHERE
    PL_number = ?
    AND
    Licenced_name = ?
    AND
    Active_ingredient = ?
    AND
    Quantity = ?
    AND
    Grant_Date = ?
"""
# SQL queries ---------------------------------------------------------------


# Functions ----------------------------------------------------------------
def error_logger(error_found):

    current_time = time.time()
    now = datetime.now()
    formatted_date = now.strftime("%d-%m-%y")
    with open(f'{formatted_date}_mhra_error_logs.txt', 'w') as error_logger:
        error_logger.write("MHRA error emailer failed" + str(current_time) + '\n' + error_found)

def email_error(error_found):
    print(error_found)
    try:
        # Create an EmailMessage object
        msg = EmailMessage()

        # Set email headers and body
        msg['Subject'] = 'Error checker'
        msg['From'] = os.getenv('EMAIL_ADDRESS_FROM')
        msg['To'] = os.getenv('EMAIL_ADDRESS_TO')
        msg.set_content("MHRA error: \n" + error_found)

        # Generate a unique Message-ID
        hostname = socket.gethostname()
        current_time = time.time()
        msg_id = f"<{int(current_time)}@{hostname}>"
        msg['Message-ID'] = msg_id

        # Create an SMTP object and specify the server and port
        server = smtplib.SMTP('smtp.enta.net', 25)
        server.starttls()  # Upgrade the connection to secure, if supported

        # Send the email
        try:
            server.send_message(msg)
        except:
            with open('mhra_error_logs.txt', 'w') as error_logger:
                error_logger.write("MHRA error emailer failed" + str(current_time))

        print("Email sent successfully!")
    except Exception as e:
        error_logger(e)
        print(f"Failed to send email: {e}")
    finally:
        server.quit()


def connect_to_db():
    '''
    open the connection to the database
    Parameters:
    cursor: Cursor
        The database cursor object

    conn : Connection
        The database connection object

    returns - nothing
    '''
    MY_SERVER = os.getenv('MY_SERVER')
    LIVE_DB = os.getenv('LIVE_DB')
    
    try:
        conn = pyodbc.connect('Driver={SQL Server};'+MY_SERVER+LIVE_DB+'Trusted_Connection=yes;')
        cursor = conn.cursor()
        return cursor,conn
    except:
    	print("DB connection failed")

def close_db_connection(cursor,conn):
    '''
    close the connection to the database
    Parameters:
    cursor: Cursor
    	The database cursor object

    conn : Connection
        The database connection object

    returns - nothing
	 '''
    try:
        cursor.close()
        conn.close()
    except:
        print('DB connection close failed')


def get_the_latest_pdf_url(url):
    pdf_urls = []
    # Send a GET request to the URL
    response = requests.get(url)   
    if response.status_code == 200:
        # Parse the HTML content of the page
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all the anchor tags in the HTML
        try:
            anchors = soup.find_all('a')
        except:
            print("No anchor tags found in the MHRA file")
            email_error("No anchor tags found in the MHRA file")
            return None

        # Extract the href attribute (URL) from each anchor tag
        urls = [a['href'] for a in anchors if 'href' in a.attrs]
        
        for url in urls:
            if "marketing_authorisations_granted" in  url.lower():
                pdf_urls.append(url)

            if "parallel_import_licences_granted" in  url.lower():
                pdf_urls.append(url)
        try:
            latest_pdf_url = pdf_urls[0]
        except:
            print("Failed to get the latest PDF url")
            email_error("Failed to get the latest PDF url")
            return None
        
        return latest_pdf_url
    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")
        email_error(f"Failed to retrieve the MHRA webpage. Status code: {response.status_code}")
        return None

def download_pdf(latest_pdf_url, save_path):
    if latest_pdf_url:
        
        try:
            file_renamed = latest_pdf_url.split('/')[-1]
        except:
            print("Failed to split the PDF name in the MHRA file")
            email_error("Failed to split the PDF name in the MHRA file")

        # Download the PDF file
        pdf_response = requests.get(latest_pdf_url)
        if pdf_response.status_code == 200:
            # Save the PDF file to disk
            new_file_path = save_path+'\\'+ file_renamed
            with open(new_file_path, 'wb') as f:
                f.write(pdf_response.content)
                print("PDF file saved successfully")
        else:
            email_error(f"Failed to download the PDF file. Status code: {pdf_response.status_code}")
            print(f"Failed to download the PDF file. Status code: {pdf_response.status_code}")
    return new_file_path


def create_data_frames(new_file_path):
    converted_dataframes = []
    try:
        tables = tabula.read_pdf(new_file_path, pages='all', multiple_tables=True)
    except:
        print("Failed to read the PDF file")
        email_error("Failed to read the PDF file")
        return None   

    if tables:
        table_count = len(tables)
        for table in range(table_count):
            df = tables[table]
            # save the file path in the dataframe
            df["File"] = new_file_path

            # check if the list is a PI list and change type if it is
            if 'Parallel' in new_file_path:
                df['Type'] = 'PI'
            else:
                df['Type'] = 'MA'

            for item in df.columns:
                # if 'Unnamed' in item remove that column
                if 'Unnamed' in item:
                    df = df.drop(item, axis=1)

            if 'PL Number' not in df.columns:
                df.columns.values[0] = 'PL Number'

            if 'Grant Date' not in df.columns:
                df.columns.values[1] = 'Grant Date'

            if 'MA Holder' not in df.columns:
                df.columns.values[2] = 'MA Holder'

            if 'Licensed Name(s)' not in df.columns:
                df.columns.values[3] = 'Licensed Name(s)'

            if 'Active Ingredient' not in df.columns:
                df.columns.values[4] = 'Active Ingredient'

            if 'Quantity' not in df.columns:
                df.columns.values[5] = 'Quantity'

            if 'Units' not in df.columns:
                df.columns.values[6] = 'Units'

            if 'Legal Status' not in df.columns:
                df.columns.values[7] = 'Legal Status'

            # Territory is not in the PI files
            if 'Parallel' not in new_file_path:
                if 'Territory' not in df.columns:
                    df.columns.values[8] = 'Territory'
            else:
                df['Territory'] = 'PI'

            if 'work type' not in df.columns:
                df['work type'] = None

            if 'auth status' not in df.columns:
                df['auth status'] = None
            
            converted_dataframes.append(df)

    return  converted_dataframes

def insert_data(all_data_frames):
    cursor,conn = connect_to_db()
    # Insert the data into the database
    for actual_data in all_data_frames:
        try:
            for i, row in enumerate(actual_data.iterrows(), start=1):
                print(row[1])
                # Check if 'PL Number' is NaN and skip if true
                if pd.isna(row[1]['PL Number']):
                    print(f"Skipping row {i} because PL Number is NaN")
                    email_error(f"Skipping row {row} PL Number is NaN")

                    continue  # Skip to the next row
                if pd.isna(row[1]['Licensed Name(s)']):
                    print(f"Skipping row {i} because PL Number is NaN")
                    email_error(f"Skipping row {row} because Licensed Name(s) is NaN")
                    continue  # Skip to the next row

                try:
                    grant_date = datetime.strptime(str(row[1]['Grant Date']), '%d/%m/%Y').date()
                    grant_date = grant_date.strftime('%Y-%m-%d')  # Convert to string in the format SQL Server expects

                except ValueError as e:
                    with open('mhra_error_logs.txt', 'a') as error_logger:
                        error_logger.write('file: '+ str(row[1]['File']) +'\n')
                        error_logger.write('date format error: '+str(row[1]['Grant Date']) + '\n')
                    print("Date format error for row {i} with date {row[1]['Grant Date']}: {e}")
                try:
                    quantity = float(row[1]['Quantity'])
                except ValueError as e:
                    with open('mhra_error_logs.txt', 'a') as error_logger:
                        error_logger.write('file: '+ str(row[1]['File']) +'\n')
                        error_logger.write('quantity format error: '+str(row[1]['Quantity']) + '\n')
                    print(f"Quantity format error for row {i} with quantity {row[1]['Quantity']}: {e}")
                    quantity = float(999.999)
                data_tuple = (
                        row[1]['PL Number'], 
                        grant_date,
                        row[1]['MA Holder'], 
                        row[1]['Licensed Name(s)'], 
                        row[1]['Active Ingredient'],
                        quantity, 
                        row[1]['Units'], 
                        row[1]['Legal Status'], 
                        row[1]['work type'],
                        row[1]['auth status'],
                        row[1]['Territory'],
                        row[1]['Type']
                    )

                cursor.execute(sql_select_query, (row[1]['PL Number'], row[1]['Licensed Name(s)'], row[1]['Active Ingredient'], float(row[1]['Quantity']), grant_date))
                if cursor.fetchone():
                    print(f"Row {i} already exists in the database")
                    with open('mhra_error_logs.txt', 'a') as error_logger:
                        error_logger.write('file: '+ str(row[1]['File']) +'\n')
                        error_logger.write(f"already in db: {row[1]['PL Number']} {row[1]['Licensed Name(s)']} {row[1]['Active Ingredient']} {row[1]['Quantity']}\n")

                    continue
                else:
                    cursor.execute(sql_insert_query, data_tuple)
                    conn.commit()
                    print(f"Row {i} inserted successfully")

        except Exception as e:
            conn.rollback()
            with open('mhra_error_logs.txt', 'a') as error_logger:
                error_logger.write('file: '+ str(row[1]['File'])+ 'Failed somewhere, the last line was' +'\n')
                error_logger.write(f"already in db: {row}\n")
            print(f"An error occurred: {e}")

    close_db_connection(cursor,conn)
# Functions ----------------------------------------------------------------

def main():
    # Step 1: get the latest pdf url
    latest_pdf_url = get_the_latest_pdf_url(full_url)
    # Step 2: download the pdf
    new_file_path = download_pdf(latest_pdf_url, save_directory_path)

    # Step 3: sleep to make sure the file is downloaded
    time.sleep(5)

    # Step 4: create dataframes
    if not os.path.exists(save_directory_path):
        os.makedirs(save_directory_path)
    if latest_pdf_url:
        file_name = latest_pdf_url.split('/')[-1]
        all_data_frames = create_data_frames(new_file_path)

    else:
        email_error("Failed to create the dataframes for the PI files")
        print("Failed to create the dataframes for the PI files")


    # step 5: send data to the database
    insert_data(all_data_frames)
    # sleep to make sure the data is inserted
    time.sleep(5)

    # PI version
    # # Step 1: get the latest pdf url
    pi_latest_pdf_url = get_the_latest_pdf_url(pi_full_url)
    # # Step 2: download the pdf 
    pi_new_file_path = download_pdf(pi_latest_pdf_url, pi_save_directory_path)
    # Step 3: sleep to make sure the file is downloaded
    time.sleep(5)

    # Step 4: create dataframes
    if not os.path.exists(pi_save_directory_path):
        os.makedirs(pi_save_directory_path)
    if pi_latest_pdf_url:
        pi_file_name = pi_latest_pdf_url.split('/')[-1]

        pi_all_data_frames = create_data_frames(pi_new_file_path)
    else:
        email_error("Failed to create the dataframes for the PI files")
        print("Failed to create the dataframes for the PI files")

    # step 5: send data to the database
    insert_data(pi_all_data_frames)
    
if __name__ == "__main__":
    main()
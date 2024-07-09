from urllib import response
import requests
import json
import os
import zipfile

def get_latest_release():
    '''This function will get the latest release of the NHSBSA dm+d
        API Key is required to access the data
    '''
    
    # this is for the latest release of the NHSBSA dm+d
    latest = "https://isd.digital.nhs.uk/trud/api/v1/keys/" + os.getenv('API_KEY') + "/items/24/releases?latest"
    
    # Perform the GET request
    response = requests.get(latest)

    # Check the response status code
    if response.status_code == 200:
        # Try to parse the response as JSON
        try:
            json_data = response.json()
            return json_data
        except json.decoder.JSONDecodeError:
            # Handle JSON decoding error
            print("Response is not in JSON format.")
            print(response.text)
    else:
        print("Failed to retrieve data: Status code", response.status_code)
        print(response.text)



def download_zip_file(url):
    '''This function will download the zip file from the url provided'''
    filename = url.split('/')[-1]
    # Make the GET request
    response = requests.get(url, stream=True)
    print("downloading: %s" % filename + "...")
    
     # Open the file in write-binary mode and write the contents
    with open(filename, 'wb') as file:
        for chunk in response.iter_content(chunk_size=128):
            file.write(chunk)

    print(f"File downloaded and saved as {filename}")
    
    
def extract_zip_file(filename):
    '''This function will extract the zip file to a directory'''
    
    # Define the path to the ZIP file
    zip_file_path = filename

    # Define the directory where you want to extract the files
    extract_to_directory = os.getenv('EXTRACT_TO_DIRECTORY')

    # Check if the ZIP file exists
    if not os.path.exists(zip_file_path):
        print(f"ZIP file not found: {zip_file_path}")
    else:
        # Open the ZIP file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            # Extract all the contents into the directory
            zip_ref.extractall(extract_to_directory)
            print(f"Files extracted to {extract_to_directory}")
    

def main():
    '''This function will call the functions to get the latest release, download the zip file and extract the zip file'''
    data = get_latest_release()

    latest_file = data['releases'][0]['id']
    date_of_file = data['releases'][0]['releaseDate']
    ulr_for_latest_file = data['releases'][0]['archiveFileUrl']
    print(f"latest file: {latest_file} and date of file: {date_of_file} and url for latest file: {ulr_for_latest_file}")

    download_zip_file()
    extract_zip_file()

if __name__ == "__main__":
    main()
# call the functions


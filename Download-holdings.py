import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import os
from datetime import datetime
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
from pathlib import Path
from urllib.parse import unquote

ETFs = ['APLY', 'ABNY', 'AIYY', 'AMDY', 'AMZY', 'OARK', 'BABO', 'YBIT', 'CONY', 
        'DISO', 'FEAT', 'FIVY', 'GOOY', 'GDXY', 'JPMO', 'MARO', 'FBY', 'MRNY', 
        'MSFO', 'MSTY', 'YMAG', 'NFLY', 'NVDY', 'PLTY', 'PYPY', 'SMCY', 'SNOY', 
        'SQY', 'FIAT', 'YQQQ', 'DIPS', 'CRSH', 'TSLY', 'TSMY', 'BIGY', 'SOXY', 
        'ULTY', 'YMAX', 'XOMO']

current_folder = Path(__file__).resolve().parent

TODAY=datetime.now().date().strftime("%Y-%m-%d")
folder = os.path.join(current_folder,"yieldmax",TODAY)
#create folder if not exists
if not os.path.exists(folder):
    os.makedirs(folder)



def selenium_download(url):
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": folder,  # Replace with your desired path
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration
    chrome_options.add_argument("--no-sandbox")  # Disable sandboxing
    chrome_options.add_argument("--disable-dev-shm-usage")  # Disable shared memory usage
    chrome_options.add_argument("--remote-debugging-port=9222") 

    # Initialize WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    # Navigate to the CSV file URL directly
    #csv_url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT28cQMYy4k0UD9DbpVVeg2EDIDNCurCeqenrDZfX849izXsk0sBGC1yfDKOeIkre0Ec9hRQ0i1Q_jn/pub?gid=0&single=true&output=csv"
    driver.get(url.replace("#038;", "&"))

    # Wait for the download to complete
    time.sleep(5)

    # Close the browser
    driver.quit()





for etf in ETFs:
    # URL of the webpage
    page_url = f"https://www.yieldmaxetfs.com/our-etfs/{etf.lower()}/"

    # Send a GET request to the webpage
    response = requests.get(page_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Find the "Download All Holdings" button by its text
        download_buttons = soup.find_all("a", class_="elementor-button elementor-button-link elementor-size-sm")
        

        for download_button in download_buttons:
            if download_button:
                # Extract the href attribute (the download URL)
                download_url = download_button.get("href")

                if "holdings" not in download_url or etf not in download_url:
                    continue
                
                # Ensure the URL is absolute
                if not download_url.startswith("http"):
                    download_url = f"https://www.yieldmaxetfs.com{download_url}"
                
                # Download the CSV file
                csv_response = requests.get(download_url)
                parsed_url = urlparse(download_url)
                filename = os.path.basename(parsed_url.path)

                if csv_response.status_code == 200:
                    # Save the content to a file
                    with open(folder+"/"+filename, "wb") as file:
                        file.write(csv_response.content)
                    print(f"{TODAY} {etf} CSV file downloaded successfully.")
                else:
                    print(f"Failed to download the CSV file. Status code: {csv_response.status_code}")
            else:
                print("Could not find the 'Download All Holdings' button on the page.")
    else:
        print(f"Failed to fetch the webpage. Status code: {response.status_code}")



#Download intraday file

intraday_url = "https://www.yieldmaxetfs.com/ym/intraday-file"


intraday_csv = requests.get(intraday_url)
if intraday_csv.status_code == 200:
    # Save the content to a file
    soup = BeautifulSoup(intraday_csv.content, "html.parser")
    script_tag = soup.find('script')
    target_url_match = re.search(r'const targetUrl = "([^"]+)"', script_tag.string)

    if target_url_match:
        target_url = target_url_match.group(1)
        print("Target URL:", target_url)
    else:
        print("Target URL not found")

    selenium_download(target_url)
    print(f"{TODAY} intraday_csv file downloaded successfully.")

else:
    print(f"Failed to download the intraday_csv file. Status code: {intraday_csv.status_code}")

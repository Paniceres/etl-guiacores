import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import logging
import re
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GuiaCoresScraper:
    def __init__(self):
        self.url = "https://www.guiacores.com.ar/index.php?r=search%2Findex&b=&R=&L=&Tm=1"
        self.driver = None
        self.setup_driver()
        self.start_time = datetime.now()
        
    def setup_driver(self):
        """Setup Chrome driver with appropriate options"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        
    def extract_business_data(self, element):
        """Extract business information from a single business element"""
        try:
            # Extract business name and link
            name_element = element.find('h2')
            name = name_element.text.strip() if name_element else ''
            detail_link = name_element.find('a')['href'] if name_element and name_element.find('a') else None
            
            # Extract phone numbers
            phones = []
            phone_elements = element.find_all('div', class_='phone')
            for phone in phone_elements:
                phones.append(phone.text.strip())
            
            # Extract address
            address = element.find('div', class_='address').text.strip() if element.find('div', class_='address') else ''
            
            # Extract basic info
            basic_info = {
                'name': name,
                'phones': ' | '.join(phones),
                'address': address,
                'detail_link': detail_link,
                'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return basic_info
        except Exception as e:
            logger.error(f"Error extracting business data: {str(e)}")
            return None

    def extract_detailed_info(self, url):
        """Extract detailed information from a business's detail page"""
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "business-detail"))
            )
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            detail_div = soup.find('div', class_='business-detail')
            
            if not detail_div:
                return {}
            
            info = {}
            
            # Extract email
            email_element = detail_div.find('a', href=re.compile(r'^mailto:'))
            info['email'] = email_element['href'].replace('mailto:', '') if email_element else 'N/A'
            
            # Extract website
            website_element = detail_div.find('a', href=re.compile(r'^https?://'))
            info['website'] = website_element['href'] if website_element else 'N/A'
            
            # Extract social media
            facebook_element = detail_div.find('a', href=re.compile(r'facebook\.com'))
            instagram_element = detail_div.find('a', href=re.compile(r'instagram\.com'))
            
            info['facebook'] = facebook_element['href'] if facebook_element else 'N/A'
            info['instagram'] = instagram_element['href'] if instagram_element else 'N/A'
            
            # Extract business hours
            hours_element = detail_div.find('div', class_='business-hours')
            info['hours'] = hours_element.text.strip() if hours_element else 'N/A'
            
            # Extract categories/rubros
            categories_element = detail_div.find('div', class_='categories')
            info['categories'] = categories_element.text.strip() if categories_element else 'N/A'
            
            # Extract additional information
            info['description'] = detail_div.find('div', class_='description').text.strip() if detail_div.find('div', class_='description') else 'N/A'
            info['services'] = detail_div.find('div', class_='services').text.strip() if detail_div.find('div', class_='services') else 'N/A'
            
            # Extract location coordinates if available
            map_element = detail_div.find('div', class_='map')
            if map_element:
                info['latitude'] = map_element.get('data-lat', 'N/A')
                info['longitude'] = map_element.get('data-lng', 'N/A')
            
            return info
            
        except Exception as e:
            logger.error(f"Error extracting detailed info from {url}: {str(e)}")
            return {}

    def scrape_page(self):
        """Scrape the current page and return business data"""
        try:
            # Wait for the business listings to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "business-listing"))
            )
            
            # Get page source and parse with BeautifulSoup
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            business_elements = soup.find_all('div', class_='business-listing')
            
            businesses = []
            for element in business_elements:
                business_data = self.extract_business_data(element)
                if business_data and business_data['detail_link']:
                    # Get detailed information
                    detailed_info = self.extract_detailed_info(business_data['detail_link'])
                    business_data.update(detailed_info)
                    businesses.append(business_data)
                    time.sleep(1)  # Be nice to the server
                    
            return businesses
        except Exception as e:
            logger.error(f"Error scraping page: {str(e)}")
            return []

    def click_next_page(self):
        """Click the 'Ver más' button to load more results"""
        try:
            ver_mas_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "ver-mas"))
            )
            ver_mas_button.click()
            time.sleep(2)  # Wait for new content to load
            return True
        except Exception as e:
            logger.error(f"Error clicking next page: {str(e)}")
            return False

    def scrape_all(self):
        """Scrape all pages and save to CSV"""
        try:
            self.driver.get(self.url)
            all_businesses = []
            page_count = 0
            
            while True:
                logger.info(f"Scraping page {page_count + 1}")
                businesses = self.scrape_page()
                all_businesses.extend(businesses)
                
                if not self.click_next_page():
                    break
                    
                page_count += 1
                
            # Convert to DataFrame and save to CSV
            df = pd.DataFrame(all_businesses)
            
            # Add extraction metadata
            df['extraction_start'] = self.start_time.strftime('%Y-%m-%d %H:%M:%S')
            df['extraction_end'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Ensure data directory exists
            os.makedirs('data', exist_ok=True)
            
            # Save to CSV
            df.to_csv('data/guiaCores_leads.csv', index=False, encoding='utf-8')
            logger.info(f"Scraped {len(all_businesses)} businesses successfully")
            
        except Exception as e:
            logger.error(f"Error in main scraping process: {str(e)}")
        finally:
            if self.driver:
                self.driver.quit()

if __name__ == "__main__":
    scraper = GuiaCoresScraper()
    scraper.scrape_all() 
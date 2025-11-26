from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def debug_selectors():
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=options)
    
    try:
        print("Navigating to search page...")
        # Use the URL with Tm=1 as base, but we want to see if we can find the "coincidencias" text
        # We'll try to search for something simple or just load the page if it shows results by default
        driver.get("https://www.guiacores.com.ar/index.php?r=search%2Findex&b=&R=&L=&Tm=1")
        
        # Wait for some content
        time.sleep(5)
        
        # Print text that might contain "coincidencias"
        body_text = driver.find_element(By.TAG_NAME, "body").text
        if "coincidencias" in body_text:
            print("Found 'coincidencias' in body text.")
            # Try to find the element
            elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'coincidencias')]")
            for el in elements:
                print(f"Element tag: {el.tag_name}, Class: {el.get_attribute('class')}, Text: {el.text}")
                print(f"Parent HTML: {el.find_element(By.XPATH, '..').get_attribute('outerHTML')}")
        else:
            print("'coincidencias' not found in initial page text.")

        # Check for "con mail" checkbox in advanced search
        print("\nChecking for 'con mail' input...")
        try:
            # Open advanced search
            driver.find_element(By.CSS_SELECTOR, 'a[data-target="#formBusquedaAvazada"]').click()
            time.sleep(2)
            
            modal = driver.find_element(By.ID, "formBusquedaAvazada")
            print(f"Modal HTML: {modal.get_attribute('outerHTML')[:1000]}...") # Print first 1000 chars
            
            # Look for inputs inside modal
            inputs = modal.find_elements(By.TAG_NAME, "input")
            for inp in inputs:
                print(f"Input: type={inp.get_attribute('type')}, name={inp.get_attribute('name')}, id={inp.get_attribute('id')}")
                
        except Exception as e:
            print(f"Error checking advanced search: {e}")

    finally:
        driver.quit()

if __name__ == "__main__":
    debug_selectors()

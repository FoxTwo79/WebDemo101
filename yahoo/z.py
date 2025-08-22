from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import time
import tempfile
import os
import random

def get_crumb_with_selenium_fixed():
    """Fixed Selenium approach with better error handling"""
    print("🚀 Starting Selenium browser...")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # New headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Add more stability options
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Create a truly unique temp directory with random name
    random_id = random.randint(1000, 9999)
    temp_dir = f"/tmp/chrome_temp_{random_id}"
    os.makedirs(temp_dir, exist_ok=True)
    chrome_options.add_argument(f"--user-data-dir={temp_dir}")
    
    try:
        print("📋 Initializing Chrome driver...")
        # Add service configuration
        from selenium.webdriver.chrome.service import Service
        service = Service()
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        try:
            print("🌐 Navigating to Yahoo Finance...")
            driver.get("https://finance.yahoo.com/quote/AAPL")
            
            # Wait for page to load with longer timeout
            print("⏳ Waiting for page to load (up to 30 seconds)...")
            try:
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                print("✅ Page loaded successfully")
            except TimeoutException:
                print("⚠️  Page load timed out, but continuing...")
            
            # Get basic page info
            print(f"📄 Current URL: {driver.current_url}")
            print(f"📝 Page title: {driver.title[:50]}...")
            
            # Wait a bit more for JavaScript to execute
            time.sleep(3)
            
            # Get cookies
            print("🍪 Collecting cookies...")
            cookies = driver.get_cookies()
            cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}
            print(f"✅ Got {len(cookies)} cookies")
            
            # Try to get crumb using JavaScript execution
            print("🔍 Attempting to get crumb via JavaScript...")
            
            # First try the direct URL
            driver.get("https://query1.finance.yahoo.com/v1/test/getcrumb")
            
            # Wait for response
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Get page content
                page_text = driver.find_element(By.TAG_NAME, "body").text
                print(f"📝 Response text: '{page_text}'")
                
                if page_text.strip():
                    crumb = page_text.strip()
                    print(f"✅ Success! Crumb: '{crumb}'")
                    return crumb, cookie_dict
                else:
                    print("❌ Empty response from crumb endpoint")
                    
            except TimeoutException:
                print("❌ Timeout waiting for crumb response")
                # Check page source for clues
                page_source = driver.page_source[:200]
                print(f"📄 Page source snippet: {page_source}")
                
            return None, None
                
        except Exception as e:
            print(f"❌ Error during browsing: {e}")
            return None, None
                
        finally:
            print("🔚 Closing browser...")
            try:
                driver.quit()
                print("✅ Browser closed successfully")
            except:
                print("⚠️  Error closing browser, but continuing...")
            
    except WebDriverException as e:
        print(f"❌ WebDriver error: {e}")
        # Try without user-data-dir as fallback
        print("🔄 Trying without user-data-dir...")
        return get_crumb_without_user_data()
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None, None
        
    finally:
        # Clean up temp directory
        try:
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
            print("🧹 Temporary directory cleaned up")
        except:
            print("⚠️  Could not clean up temp directory")

def get_crumb_without_user_data():
    """Alternative approach without user-data-dir"""
    print("🔄 Trying without user-data-dir...")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        
        try:
            driver.get("https://query1.finance.yahoo.com/v1/test/getcrumb")
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            crumb = driver.find_element(By.TAG_NAME, "body").text.strip()
            print(f"✅ Success without user-data-dir! Crumb: '{crumb}'")
            return crumb, {}
            
        finally:
            driver.quit()
            
    except Exception as e:
        print(f"❌ Failed without user-data-dir: {e}")
        return None, None

# SIMPLER ALTERNATIVE: Use requests with manual cookie handling
import requests

def get_crumb_simple_requests():
    """Simple requests-based approach"""
    print("🍪 Trying simple requests approach...")
    
    session = requests.Session()
    
    # Set realistic headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    # First, get cookies from main page
    print("🌐 Getting initial cookies...")
    try:
        response = session.get('https://finance.yahoo.com/', headers=headers, timeout=15)
        print(f"✅ Main page: {response.status_code}")
    except Exception as e:
        print(f"❌ Error with main page: {e}")
        return None
    
    # Now try crumb endpoint
    print("🔍 Trying crumb endpoint...")
    try:
        response = session.get(
            'https://query1.finance.yahoo.com/v1/test/getcrumb',
            headers=headers,
            timeout=15
        )
        
        print(f"📊 Crumb status: {response.status_code}")
        crumb_text = response.text.strip()
        print(f"📝 Crumb response: '{crumb_text}'")
        
        if response.status_code == 200 and crumb_text:
            print(f"✅ Success! Crumb: '{crumb_text}'")
            return crumb_text
        else:
            print("❌ Invalid response")
            return None
            
    except Exception as e:
        print(f"❌ Error getting crumb: {e}")
        return None

# TEST FUNCTION
def test_all_methods():
    """Test all available methods"""
    print("=" * 60)
    print("TESTING YAHOO FINANCE CRUMB ACCESS")
    print("=" * 60)
    
    # Method 1: Simple requests
    print("\n1. 🍪 Testing Simple Requests Method...")
    crumb = get_crumb_simple_requests()
    if crumb:
        print(f"🎉 SUCCESS with Simple Requests: {crumb}")
        return crumb
    
    # Method 2: Selenium with fixes
    print("\n2. 🚀 Testing Selenium Method...")
    crumb, cookies = get_crumb_with_selenium_fixed()
    if crumb:
        print(f"🎉 SUCCESS with Selenium: {crumb}")
        return crumb
    
    # Method 3: Direct endpoint test
    print("\n3. 🔗 Testing Direct Endpoint...")
    try:
        response = requests.get(
            'https://query1.finance.yahoo.com/v1/test/getcrumb',
            headers={'User-Agent': 'Mozilla/5.0'},
            timeout=10
        )
        print(f"📊 Direct test status: {response.status_code}")
        if response.status_code == 200:
            crumb = response.text.strip()
            print(f"🎉 SUCCESS with Direct: {crumb}")
            return crumb
    except Exception as e:
        print(f"❌ Direct test failed: {e}")
    
    print("\n💥 All methods failed - Yahoo might be blocking your IP")
    return None

if __name__ == "__main__":
    print("Starting Yahoo Finance crumb test...")
    result = test_all_methods()
    
    if result:
        print(f"\n✅ FINAL RESULT: Got crumb - {result}")
    else:
        print("\n❌ FINAL RESULT: Failed to get crumb")
        
    print("\nTest completed!")
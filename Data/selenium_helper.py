from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.remote.webelement import WebElement
# from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
# from fake_useragent import UserAgent
import time

class helper:

    def __init__(self, user_data) -> None:

        self.ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'

        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--user-agent=" + self.ua)

        if user_data == True: 
            options.add_argument(r"--user-data-dir=C:\Users\Rizwan\AppData\Local\Google\Chrome\User Data")
        
        self.driver = webdriver.Chrome(service=Service(r'C:/Users/Rizwan/OneDrive/Desktop/chromedriver.exe'), options=options)
        self.KEYS = Keys
    
    def locate(self, path, wait_time=10, group=False, exception=True):
        try:
            element = WebDriverWait(self.driver, wait_time).until(EC.presence_of_element_located((By.XPATH, path)))
            if group:
                return element.find_elements(By.XPATH, path)
            else:
                return element.find_element(By.XPATH, path)
        except Exception as e:
            if exception == True:
                print(f'Failed to locate {path} in the given duration of {wait_time} with the exception...\n{e}')


    def click(self, path, wait_time=10, delay=0, exception=True):
        try:
            elem = WebDriverWait(self.driver, wait_time).until(EC.presence_of_element_located((By.XPATH, path)))
            if delay != 0: 
                time.sleep(delay)
                elem.click()
            else: elem.click()
        except Exception as e:
            if exception == True:
                print(f'Failed to click at region {path} in the given duration of {wait_time} with the exception...\n{e}')


    def type_(self, path, text, wait_time=10, delay=0.0):
        try:
            elem = WebDriverWait(self.driver, wait_time).until(EC.presence_of_element_located((By.XPATH, path)))
            if delay != 0.0:
                for character in text:
                    elem.send_keys(character)
                    time.sleep(delay)
            else:
                elem.send_keys(text)
        except Exception as e:
            print(f'Failed to type {text} at region {path} in the given duration of {wait_time} with the exception...\n{e}')


class GetInfo(helper):

    def __init__(self, user_data) -> None:
        super().__init__(user_data)

    def get_cookies(self):
        return self.driver.get_cookies()

    def monitor():
        return None
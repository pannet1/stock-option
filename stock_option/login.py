import requests,json
from kiteconnect import KiteConnect, KiteTicker
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import time
import pyotp

def kiteLogin(user_id, user_pwd, totp_key ,api_key, api_secret):
    driver = webdriver.Chrome(r'C:\Users\Administrator\Desktop\chromedriver.exe')
    driver.get(f'https://kite.trade/connect/login?api_key={api_key}&v=3')
    login_id = WebDriverWait(driver, 1).until(lambda x: x.find_element_by_xpath('//*[@id="userid"]'))
    login_id.send_keys(user_id)
    pwd = WebDriverWait(driver, 1).until(lambda x: x.find_element_by_xpath('//*[@id="password"]'))
    pwd.send_keys(user_pwd)

    submit = WebDriverWait(driver, 1).until(lambda x: x.find_element_by_xpath('//*[@id="container"]/div/div/div[2]/form/div[4]/button'))
    submit.click()

    time.sleep(1)
    totp = WebDriverWait(driver, 1).until(lambda x: x.find_element_by_xpath('//*[@icon="shield"]'))
    authkey = pyotp.TOTP(totp_key)
    totp.send_keys(authkey.now())

    # continue_btn = WebDriverWait(driver, 10).until(lambda x: x.find_element_by_xpath('//*[@id="container"]/div/div/div[2]/form/div[3]/button'))
    # continue_btn.click()

    time.sleep(5)
    print("im here")
    url = driver.current_url
    initial_token = url.split('request_token=')[1]
    request_token = initial_token.split('&')[0]
    
    driver.close()
    
    kite = KiteConnect(api_key = api_key)
    #print(request_token)
    #data = kite.generate_session(request_token, api_secret=api_secret)
    #kite.set_access_token(data['access_token'])
    print("request_token  ",request_token)
    return request_token

try:print("zerodha login")#print(account().ltp("NSE:INFY"))
except:print("")#login()
if __name__=="__main__": pass


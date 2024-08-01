import csv
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
import time

def read_credentials():
    with open('credentials.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            return row['email'], row['password']

def read_times():
    with open('times.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            return row['start_time'], row['end_time']

def write_kintai_info(start_time, end_time):
    date = datetime.datetime.now().strftime("%Y-%m-%d")
    work_hours = calculate_work_hours(start_time, end_time)
    with open('勤怠情報.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([date, start_time, end_time, work_hours])

def calculate_work_hours(start_time, end_time):
    fmt = '%H:%M'
    tdelta = datetime.datetime.strptime(end_time, fmt) - datetime.datetime.strptime(start_time, fmt)
    hours = tdelta.total_seconds() / 3600 - 1  # Subtracting 1 hour for lunch
    return f"{hours}h"

try:
    email, password = read_credentials()
    start_time, end_time = read_times()

    browser = webdriver.Edge()

    # Webページを開く
    browser.get("https://ssl.jobcan.jp/login/mb-employee?client_id=nex0729&lang_code=ja")

    # 検索ボックスを特定し、キーワードを入力
    mail_box = browser.find_element(By.NAME, "email")
    mail_box.send_keys(email)

    password_box = browser.find_element(By.NAME, "password")
    password_box.send_keys(password)

    time.sleep(3)  # このsleepは本来は必要ない（デモ用）

    # ボタンを特定してクリック（CLASS_NAMEを用いた例）
    button = browser.find_element(By.CLASS_NAME, 'btn-info')  
    button.click()

    # もしくはCSSセレクタを使用してもよい
    # button = browser.find_element(By.CSS_SELECTOR, '.btn.btn-info.btn-block')
    # button.click()

    time.sleep(5)  # このsleepは本来は必要ない（デモ用）

    button2=browser.find_element(By.XPATH,'//*[@id="container"]/div[7]/h3')
    button2.click()
    time.sleep(3)

    button3 = browser.find_element(By.LINK_TEXT,"修正申請")
    button3.click()
    time.sleep(3)

    kintai_input=browser.find_element(By.XPATH,'//*[@id="container"]/div[4]/table/tbody/tr[2]/td/a/li/em')
    kintai_input.click()

    in_time=browser.find_element(By.NAME,"time1")
    in_time.send_keys(start_time.replace(":", ""))

    out_time=browser.find_element(By.NAME,"time2")
    out_time.send_keys(end_time.replace(":", ""))

    time.sleep(3)

    shinsei_button=browser.find_element(By.XPATH,'//*[@id="container"]/div[4]/form/input[6]')
    shinsei_button.click()
    time.sleep(3)

    write_kintai_info(start_time, end_time)
    browser.quit()

except Exception as e:
    print(f"An error occurred: {e}")

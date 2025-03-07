import PySimpleGUI as sg
import threading
import requests
from bs4 import BeautifulSoup
from PIL import Image
import os
import time
from queue import Queue, Empty
import pandas as pd

def convert_webp_to_jpg(save_path):
    files = os.listdir(save_path)
    for file in files:
        if file.endswith('.webp'):
            webp_path = os.path.join(save_path, file)
            img = Image.open(webp_path).convert('RGB')
            jpg_path = os.path.join(save_path, file.replace('.webp', '.jpg'))
            img.save(jpg_path, 'JPEG')
            os.remove(webp_path)

def scraping(url, save_path, queue):
    soup = BeautifulSoup(requests.get(url).content, 'lxml')
    srcs = [link.get('src') for link in soup.find_all('img') if link.get('src').endswith(('.jpg', '.png', '.webp'))]
    total_images = len(srcs)
    for i, image in enumerate(srcs, start=100):
        response = requests.get(image)
        with open(os.path.join(save_path, f'{i}' + image.split('/')[-1]), 'wb') as f:
            f.write(response.content)
            time.sleep(3)
        progress = ((i - 99) / total_images) * 100
        queue.put(progress)
    convert_webp_to_jpg(save_path)
    queue.put('COMPLETE')

def start_scraping(url,save_path, queue):
    soup = BeautifulSoup(requests.get(url).content, 'lxml')
    title = soup.find('h1').text
    # 取得元のページのdiv class_titleを確認する
    pic_detail = soup.find_all('div', class_='post-tags')
    pic_dic = {}
    for i in range(len(pic_detail)-1):
        selector = 'div:nth-child('+str(i+1)+') > div.post-tag-title'
        table_title = soup.select_one(selector).text
        if table_title == 'パロディ':
            pic_dic['parody'] = pic_detail[i].text
        elif table_title == 'サークル':
            pic_dic['circle'] = pic_detail[i].text
        elif table_title == '作者':
            pic_dic['author'] = pic_detail[i].text
        elif table_title == 'キャラクター':
            pic_dic['character'] = pic_detail[i].text
        else:
            pass
    
    #pic_dicのキーを確認し、circleかauthoeのキーの存在を確認し、authorがあればその値をauthorに代入、なければcircleの値を代入
    if 'author' in pic_dic.keys():
        author = pic_dic['author']
    else:
        author = pic_dic['circle']
    
    author_folder = os.path.join(save_path, author)
    # 作者名に特殊文字が含まれている場合、全角に変換

    os.makedirs(author_folder, exist_ok=True)
    
    # 作品名に特殊文字が含まれている場合、全角に変換
    title = title.replace('?', '？').replace('/', '／').replace('\\', '＼').replace(':', '：').replace('*', '＊').replace('"', '”').replace('<', '＜').replace('>', '＞').replace('|', '｜')

    work_folder = os.path.join(author_folder, title)
    
    # work_folder = work_folder.replace('?', '？').replace('/', '／').replace('\\', '＼').replace(':', '：').replace('*', '＊').replace('"', '”').replace('<', '＜').replace('>', '＞').replace('|', '｜')
    
    os.makedirs(work_folder, exist_ok=True)
    
    save_path = work_folder + "\\"
    threading.Thread(target=scraping, args=(url, save_path, queue), daemon=True).start()

layout = [
    [sg.Text('URL'), sg.InputText(key='URL')],
    [sg.Button('Start'), sg.Button('Clear'), sg.Button('Setting')],
    [sg.ProgressBar(100, orientation='h', size=(20, 20), key='progress')]
]

window = sg.Window('Webスクレイピング', layout)
progress_queue = Queue()
save_path = 'G:\\漫画一覧２\\新しいフォルダー (7)\\'  # デフォルトの保存先パス

while True:
    event, values = window.read(timeout=100)
    if event == sg.WIN_CLOSED:
        break
    elif event == 'Clear':
        for key in ['URL']:
            window[key].update('')
    elif event == 'Start':
        if all(values.values()):
            start_scraping(values['URL'], save_path, progress_queue)
        else:
            sg.popup('未入力フォームあり')
    elif event == 'Setting':
        setting_layout = [
            [sg.Text('保存先：'), sg.InputText(save_path, key='NewSavePath')],
            [sg.Button('適用'), sg.Button('キャンセル')]
        ]
        setting_window = sg.Window('設定', setting_layout)
        while True:
            event, values = setting_window.read()
            if event in (sg.WIN_CLOSED, 'キャンセル'):
                break
            elif event == '適用':
                save_path = values['NewSavePath']
                setting_window.close()
                break
        setting_window.close()

    try:
        progress = progress_queue.get_nowait()
        if progress == 'COMPLETE':
            sg.popup('取得完了!!', title='完了')
        else:
            window['progress'].update(progress)
    except Empty:
        pass

window.close()
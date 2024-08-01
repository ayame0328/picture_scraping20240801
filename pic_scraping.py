import PySimpleGUI as sg
import threading
import requests
from bs4 import BeautifulSoup
from PIL import Image
import os
import time
from queue import Queue, Empty

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

def start_scraping(url, author, title, save_path, queue):
    author_folder = os.path.join(save_path, author)
    os.makedirs(author_folder, exist_ok=True)
    work_folder = os.path.join(author_folder, title)
    os.makedirs(work_folder, exist_ok=True)
    save_path = work_folder + "\\"
    threading.Thread(target=scraping, args=(url, save_path, queue), daemon=True).start()

layout = [
    [sg.Text('URL'), sg.InputText(key='URL')],
    [sg.Text('作者名'), sg.InputText(key='Author')],
    [sg.Text('作品名'), sg.InputText(key='Title')],
    [sg.Button('Start'), sg.Button('Clear'), sg.Button('Setting')],
    [sg.ProgressBar(100, orientation='h', size=(20, 20), key='progress')]
]

window = sg.Window('Webスクレイピング', layout)
progress_queue = Queue()
save_path = ''  # デフォルトの保存先パス

while True:
    event, values = window.read(timeout=100)
    if event == sg.WIN_CLOSED:
        break
    elif event == 'Clear':
        for key in ['URL', 'Author', 'Title']:
            window[key].update('')
    elif event == 'Start':
        if all(values.values()):
            start_scraping(values['URL'], values['Author'], values['Title'], save_path, progress_queue)
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

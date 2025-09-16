"""
Streamlit app: Kobo image downloader with Username/Password authentication (using Basic Auth)

Features:
- Upload an Excel/CSV file containing Kobo Toolbox image URLs.
- User provides Kobo username and password (KF login credentials).
- Uses Basic Auth for all requests (works for media links hosted at KC domain).
- Downloads images into a folder (images_downloaded) with sequential names: bill 1, bill 2, ...
- Detects file type (jpg/png/etc) using `filetype` library or headers.
- Shows progress and allows downloading a ZIP of all images after completion.
- Saves failed links into a CSV for retry.

Requirements:
- streamlit
- pandas
- openpyxl (for .xlsx reading)
- requests
- filetype

Run: streamlit run streamlit_kobo_image_downloader.py
"""

import streamlit as st
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urlparse
import os
import mimetypes
import time
from io import BytesIO
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import filetype

# ------- Helper functions -------

def find_urls_in_df(df):
    urls = []
    for col in df.columns:
        series = df[col].astype(str).fillna("")
        for val in series:
            val = val.strip()
            if val.lower().startswith('http://') or val.lower().startswith('https://'):
                urls.append(val)
    return urls


def detect_extension(content, content_type, url):
    # Try filetype first
    kind = filetype.guess(content)
    if kind:
        return kind.extension
    # Try Content-Type
    if content_type:
        guessed = mimetypes.guess_extension(content_type.split(';')[0].strip())
        if guessed:
            return guessed.lstrip('.')
    # Fallback to URL extension
    path = urlparse(url).path
    ext2 = os.path.splitext(path)[1]
    if ext2 and len(ext2) <= 6:
        return ext2.lstrip('.')
    return 'jpg'


def download_one(session, url, dest_name, folder, timeout=20, max_retries=2):
    last_exc = None
    for attempt in range(max_retries+1):
        try:
            resp = session.get(url, stream=True, timeout=timeout)
            if resp.status_code == 200:
                content = resp.content
                content_type = resp.headers.get('Content-Type', '')
                ext = detect_extension(content, content_type, url)
                final_name = f"{dest_name}.{ext}"
                final_path = os.path.join(folder, final_name)
                with open(final_path, 'wb') as f:
                    f.write(content)
                return True, final_name, None
            else:
                last_exc = f'HTTP {resp.status_code}'
        except Exception as e:
            last_exc = str(e)
        time.sleep(0.5 * (attempt+1))
    return False, None, last_exc

# ------- Streamlit app -------

st.set_page_config(page_title='Kobo Image Downloader', layout='wide')
st.title('📥 Kobo Image Downloader (Username/Password)')
st.write('Upload an Excel/CSV file that contains Kobo Toolbox image links. The app will use your Kobo username and password to download the images.')

# Username and Password
username = st.text_input('Kobo Username', '')
password = st.text_input('Kobo Password', type='password')

concurrency = st.slider('Concurrent downloads', min_value=1, max_value=10, value=3)
timeout = st.number_input('Request timeout (seconds)', value=20, min_value=5, max_value=120)
max_retries = st.number_input('Max retries per URL', value=2, min_value=0, max_value=5)

uploaded_file = st.file_uploader('Upload Excel or CSV file with links', type=['xlsx','xls','csv'])

if uploaded_file is not None and username and password:
    try:
        if uploaded_file.name.endswith(('.xls','.xlsx')):
            df = pd.read_excel(uploaded_file)
        else:
            df = pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f'Error reading file: {e}')
        st.stop()

    st.markdown('**Preview of file**')
    st.dataframe(df.head(50))

    urls = find_urls_in_df(df)
    urls = [u for u in urls if u and len(u) > 6]
    unique_urls = list(dict.fromkeys(urls))

    st.success(f'Found {len(unique_urls)} link(s)')

    folder_name = st.text_input('Folder to save images', value='images_downloaded')
    start_index = st.number_input('Start numbering from', min_value=1, value=1)

    if st.button('Start download'):
        if not unique_urls:
            st.warning('No URLs found in file.')
        else:
            os.makedirs(folder_name, exist_ok=True)

            # Set up session with Basic Auth
            session = requests.Session()
            session.auth = HTTPBasicAuth(username, password)

            total = len(unique_urls)
            progress_bar = st.progress(0)
            status_text = st.empty()
            log_box = st.empty()

            results = []
            counter = start_index - 1

            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                future_to_info = {}
                for url in unique_urls:
                    counter += 1
                    dest_name = f'bill {counter}'
                    future = executor.submit(download_one, session, url, dest_name, folder_name, timeout, max_retries)
                    future_to_info[future] = url

                done = 0
                log_lines = []
                for future in as_completed(future_to_info):
                    url = future_to_info[future]
                    success, final_name, error = future.result()
                    done += 1
                    progress_bar.progress(done/total)

                    if success:
                        log_lines.append(f'OK: {url} -> {final_name}')
                        results.append((url, final_name, True, None))
                    else:
                        log_lines.append(f'FAIL: {url} -> {error}')
                        results.append((url, None, False, error))

                    status_text.text(f'Downloaded {done}/{total}')
                    log_box.text('\n'.join(log_lines[-50:]))

            st.success('Download finished')
            succ = sum(1 for r in results if r[2])
            fail = sum(1 for r in results if not r[2])
            st.write(f'Successful: {succ} — Failed: {fail}')

            if succ > 0:
                zip_buffer = BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    for _, fname, ok, _ in results:
                        if ok and fname:
                            fpath = os.path.join(folder_name, fname)
                            if os.path.exists(fpath):
                                zipf.write(fpath, fname)
                zip_buffer.seek(0)
                st.download_button('Download ZIP of images', data=zip_buffer, file_name=f'{folder_name}.zip')

            if fail > 0:
                failed_links = [url for url, _, ok, _ in results if not ok]
                fail_df = pd.DataFrame(failed_links, columns=['failed_url'])
                csv_buffer = BytesIO()
                fail_df.to_csv(csv_buffer, index=False)
                st.download_button('Download failed links CSV', data=csv_buffer.getvalue(), file_name='failed_links.csv', mime='text/csv')

            st.markdown('**Detailed log (last 200 entries)**')
            st.text('\n'.join(log_lines[-200:]))

else:
    st.info('Upload a file and enter your Kobo username & password to begin.')

# End of app
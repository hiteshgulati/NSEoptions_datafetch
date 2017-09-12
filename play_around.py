from hiteshutils import basicutils as bu
import requests
import urllib
from PIL import Image


def file_download(file_path, file_url):
    r = requests.get(file_url, stream=True)
    with open(file_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=256):
            fd.write(chunk)




print("Start")
exist = file_download(file_path='/Users/hiteshg/sample_file.zip',
                 file_url='https://www.nseindia.com/archives/fo/mkt/fo11092000.zip')
print(exist)

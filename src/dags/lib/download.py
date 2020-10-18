import requests
from bs4 import BeautifulSoup
import re

def download_file(url, path):
  local_fname = url.split("/")[-1]
  with requests.get(url, stream=True) as r:
    with open(path+local_fname, 'wb') as f:
      for chunk in r.iter_content(chunk_size=8192):
        f.write(chunk)
  return local_fname


def download_ppd():
  url = "https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads"
  page = requests.get(url)

  soup = BeautifulSoup(page.content, 'html.parser')
  res = soup.find_all('a', class_='govuk-link')

  urls = []
  for i in res:
    url = i.get('href')
    if url.endswith("pp-complete.csv"):
      urls.append(url)

  print(urls)
  if len(urls) == 0:
    return False
  
  # download_file(urls[0], '../data/')
  return True


# if (url.endswith(".csv") and url[-8:-4].isnumeric()):
# u1 = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv'
# r = requests.get(u1, allow_redirects=True)
# open('../d2/pp-complete.csv', 'wb').write(r.content)

# for i in urls:
#   r = requests.get(i, allow_redirects=True)
#   open('../d2/'+i[-11:], 'wb').write(r.content)



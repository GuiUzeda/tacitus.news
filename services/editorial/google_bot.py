import requests
import time

url = "https://www.metropoles.com/sitemap_index.xml"

headers = {
    # This is the "Magic Key". We pretend to be Google's crawler.
    'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
    'Accept': 'text/plain,text/html,application/xhtml+xml,application/xml',
}

# Add a delay if you are running this in a loop!
time.sleep(2) 

response = requests.get(url, headers=headers)

print(response.text) # Should be 200
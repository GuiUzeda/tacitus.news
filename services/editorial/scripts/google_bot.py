import requests
import time

url = "https://noticias.uol.com.br/sitemap/v2/news-01.xml"

headers = {
    # This is the "Magic Key". We pretend to be Google's crawler.
    "User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1",
}
# Add a delay if you are running this in a loop!
time.sleep(2)

response = requests.get(url, headers=headers)

print(response.text)  # Should be 200

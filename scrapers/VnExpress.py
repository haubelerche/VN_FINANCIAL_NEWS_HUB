import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import schedule


def scrape_vnexpress_news():
    url = 'https://vnexpress.net/kinh-doanh'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve data: {response.status_code}")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    print(f"Response status: {response.status_code}")
    print(f"Page title: {soup.title.text if soup.title else 'No title found'}")

    # Try multiple selectors for VnExpress
    articles = (soup.find_all('article', class_='item-news') or
                soup.find_all('div', class_='item-news') or
                soup.find_all('article') or
                soup.find_all('h3', class_='title-news'))

    print(f"Found {len(articles)} articles")

    data = []
    for article in articles:
        try:
            # Try different title selectors
            title_link = (article.find('h3', class_='title-news') or
                          article.find('h2') or
                          article.find('h3')).find('a') if article.find('h3', class_='title-news') or article.find(
                'h2') or article.find('h3') else article.find('a')

            if title_link:
                title = title_link.get_text().strip()
                href = title_link.get('href', '')

                if href.startswith('/'):
                    href = 'https://vnexpress.net' + href

                # Try different date selectors
                date_element = (article.find('span', class_='time') or
                                article.find('time') or
                                article.find('span', attrs={'data-time': True}))
                article_date = date_element.get_text().strip() if date_element else 'N/A'

                # Try different summary selectors
                summary_element = (article.find('p', class_='description') or
                                   article.find('div', class_='sapo') or
                                   article.find('p'))
                summary = summary_element.get_text().strip() if summary_element else 'N/A'

                if title and href and 'vnexpress.net' in href:
                    data.append({
                        'title': title,
                        'url': href,
                        'date': article_date,
                        'summary': summary[:200] + '...' if len(summary) > 200 else summary,
                        'scraped_date': time.strftime('%Y-%m-%d %H:%M:%S')
                    })

        except Exception as e:
            print(f"Error processing article: {e}")
            continue

    df = pd.DataFrame(data)
    print(f"\nCreated DataFrame with {len(df)} rows")

    # Use absolute path to avoid issues when running from different directories
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    csv_file = os.path.join(project_root, 'data', 'raw', 'VnExpress_News.csv')

    if os.path.exists(csv_file):
        existing_df = pd.read_csv(csv_file, encoding='utf-8-sig')
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['title', 'url'], keep='last')
        combined_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
    else:
        df.to_csv(csv_file, index=False, encoding='utf-8-sig')


if __name__ == "__main__":
    print("Starting VNExpress News Scraper...")
    print("Will scrape news every 30 mins")
    # Run immediately on startup
    scrape_vnexpress_news()

    # Schedule to run every hour
    schedule.every(30).minutes.do(scrape_vnexpress_news)

    print("\nScheduler started. Press Ctrl+C to stop.")

    while True:
        schedule.run_pending()
        time.sleep(10)  # Check every minute instead of every second

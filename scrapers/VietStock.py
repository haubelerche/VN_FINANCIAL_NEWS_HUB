import time
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd


def scrape_vietstock_news():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    try:
        urls = [
            "https://vietstock.vn/chu-de/1-2/moi-cap-nhat.htm",
            "https://vietstock.vn/tai-chinh.htm",
            "https://vietstock.vn/kinh-te.htm"
        ]

        all_data = []

        for url in urls:
            print(f"Scraping URL: {url}")
            driver.get(url)
            time.sleep(5)  # Increased wait time

            # Use html.parser instead of lxml for better compatibility
            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            print(f"Page title: {soup.title.text if soup.title else 'No title'}")
            print(f"Page content length: {len(driver.page_source)} characters")

            # Try multiple selectors for VietStock articles
            articles = (soup.find_all('div', class_='news-item') or 
                       soup.find_all('article') or
                       soup.find_all('div', class_='item') or
                       soup.find_all('div', class_='list-item') or
                       soup.find_all('div', class_='article') or
                       soup.find_all('li', class_='item') or
                       soup.find_all('div', attrs={'class': lambda x: x and 'item' in x}) or
                       soup.find_all('a', href=True))

            print(f"Found {len(articles)} potential articles")

            for article in articles:
                try:
                    # Handle different article structures
                    title_element = None
                    link = ''
                    
                    # If this is already a link element
                    if article.name == 'a':
                        title_element = article
                        link = article.get('href', '')
                    else:
                        # Look for title in various ways
                        title_element = (article.find('h3') or 
                                       article.find('h2') or 
                                       article.find('h4') or
                                       article.find('a', href=True))
                        
                        if title_element:
                            if title_element.name == 'a':
                                link = title_element.get('href', '')
                            else:
                                link_elem = title_element.find('a', href=True)
                                link = link_elem.get('href', '') if link_elem else ''
                    
                    if title_element:
                        title = title_element.get_text().strip()
                        
                        # Skip if title is too short or looks like navigation
                        if len(title) < 10 or any(skip_word in title.lower() for skip_word in ['trang chủ', 'đăng nhập', 'liên hệ', 'menu']):
                            continue

                        # Ensure full URL
                        if link and link.startswith('/'):
                            link = 'https://vietstock.vn' + link
                        elif link and not link.startswith('http'):
                            continue  # Skip invalid links

                        # Try multiple date selectors
                        date_element = (article.find('span', class_='time') or 
                                      article.find('time') or
                                      article.find('span', class_='date') or
                                      article.find('div', class_='time'))
                        article_date = date_element.get_text().strip() if date_element else 'N/A'

                        # Try multiple summary selectors
                        summary_element = (article.find('p') or 
                                         article.find('div', class_='summary') or
                                         article.find('div', class_='sapo') or
                                         article.find('span', class_='summary'))
                        summary = summary_element.get_text().strip() if summary_element else 'N/A'

                        # Only add if we have valid title and link
                        if title and link and 'vietstock.vn' in link:
                            all_data.append({
                                'title': title,
                                'url': link,
                                'date': article_date,
                                'summary': summary[:200] + '...' if len(summary) > 200 else summary,
                                'scraped_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                                'source_url': url
                            })

                except Exception as e:
                    print(f"Error processing article: {e}")
                    continue

        print(f"Total articles collected: {len(all_data)}")
        driver.quit()

        if len(all_data) == 0:
            print("No articles found. The website structure may have changed.")
            print("Creating empty DataFrame to avoid CSV errors...")
            # Create empty DataFrame with expected columns
            df = pd.DataFrame(columns=['title', 'url', 'date', 'summary', 'scraped_date', 'source_url'])
        else:
            df = pd.DataFrame(all_data)
            
        print(f"Created DataFrame with {len(df)} rows")

        # Use proper file path structure
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        csv_file = os.path.join(project_root, 'data', 'raw', 'VietStock_News.csv')

        # Ensure directory exists
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)

        if len(df) > 0:  # Only save if we have data
            if os.path.exists(csv_file):
                try:
                    # Check if file is empty or corrupted
                    if os.path.getsize(csv_file) == 0:
                        print("Existing CSV file is empty, creating new file...")
                        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                        print(f"Created new CSV with {len(df)} articles")
                    else:
                        existing_df = pd.read_csv(csv_file, encoding='utf-8-sig')
                        if len(existing_df) == 0:
                            print("Existing CSV has no data, replacing with new data...")
                            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                            print(f"Created new CSV with {len(df)} articles")
                        else:
                            combined_df = pd.concat([existing_df, df], ignore_index=True)
                            combined_df = combined_df.drop_duplicates(subset=['title', 'url'], keep='last')
                            combined_df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                            print(f"Appended {len(df)} articles to existing data")
                except (pd.errors.EmptyDataError, pd.errors.ParserError) as e:
                    print(f"Error reading existing CSV (possibly corrupted): {e}")
                    print("Creating new CSV file...")
                    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                    print(f"Created new CSV with {len(df)} articles")
            else:
                df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                print(f"Created new CSV with {len(df)} articles")
        else:
            print("No new data to save.")

    except Exception as e:
        print(f"Error during scraping: {e}")
        import traceback
        traceback.print_exc()
        driver.quit()


if __name__ == "__main__":
    scrape_vietstock_news()
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

# Function to scrape a single URL
def scrape_email(i):
    url = f'https://nass.gov.ng/mps/single/{i}'
    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        h3s = soup.find_all('h3')
        links = soup.find_all("a")

        isSenator = False
        clean_text = ''
        senator_name = ''

        # Check if the person is a senator
        for h3 in h3s:
            if h3.text.startswith('Sen'):
                isSenator = True
                senator_name = h3.text

        # Extract email
        for link in links:
            if 'Email' in link.text:
                clean_text = " ".join(link.text.split()).replace("Email:", '').strip()

        print(url, i, isSenator, clean_text, senator_name)
        
        if isSenator:
            return (senator_name, clean_text)  # Return result
    
    return None  # Return None if not a senator or request failed

# Run scraping in parallel
emails = []
with ThreadPoolExecutor(max_workers=20) as executor:  # Adjust max_workers as needed
    results = executor.map(scrape_email, range(2000))

# Collect non-None results
emails = [result for result in results if result]

# Convert to DataFrame
df = pd.DataFrame(emails, columns=["Name", "Email"])

# Save emails separately
df["Email"].to_csv("emails_only.csv", index=False, header=False)

# Save names and emails separately
df.to_csv("emails_with_names.csv", index=False)

print("Scraping completed! Files saved as 'emails_only.csv' and 'emails_with_names.csv'.")

import requests, re
from bs4 import BeautifulSoup
import urllib.parse

def get_redirect_link(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Check for URL in the meta refresh tag
    meta_refresh = soup.find('meta', attrs={'http-equiv': 'refresh'})
    if meta_refresh:
        content = meta_refresh.get('content')
        if content:
            match = re.search(r'url=(.*)', content, re.IGNORECASE)
            if match:
                return match.group(1)
    
    # Check for URL in the JavaScript
    scripts = soup.find_all('script')
    for script in scripts:
        if script.string:
            match = re.search(r'window\.location\.href\s*=\s*"(http.*?)"', script.string)
            if match:
                return match.group(1)
            match = re.search(r'window\.location\.href\s*=\s*\'(http.*?)\'', script.string)
            if match:
                return match.group(1)

    return None

def check_status_code(status_code):
    if status_code >= 200 and status_code < 300:
        return False
    elif status_code >= 400:
        return True
    else:
        return f"Received unexpected status code: {status_code}"

def is_lazada_affiliate_expired(link):
    try:
        response = requests.get(link, allow_redirects=True, timeout=5)
        
        # Check the final URL after redirections
        final_url = response.url
        if 'lazada' not in final_url and 'lazada' not in link and final_url == link:
            re_link = get_redirect_link(response.text)
            if not re_link:
                return f'Link({link}) is not a Lazada affiliate link.'
            decoded_url = urllib.parse.unquote(re_link)
            h_link = decoded_url.find('https://')
            re_link = decoded_url[h_link:]
            if 'lazada' not in str(re_link) and 'lazada' not in link:
                return f'Link({link}) is not a Lazada affiliate link.'
            if re_link:
                response = requests.get(re_link, allow_redirects=True, timeout=5)
            status = check_status_code(response.status_code)
            return status
        else:
            decoded_url = urllib.parse.unquote(final_url)
            h_link = decoded_url.find('https://')
            final_url = decoded_url[h_link:]
            if 'lazada' not in final_url:
                return f'Link({link}) is not a Lazada affiliate link.'
        # Make a GET request to the URL with a browser user agent
        status = check_status_code(response.status_code)
        if status:
            return status
        return False
    
    except requests.exceptions.RequestException as e:
        # Handle exceptions like connection errors
        return f"{link}: An error occurred: {e}"
    

def status_affiliate_link(link, shop='lazada'):
    if shop == 'lazada':
        is_expired = is_lazada_affiliate_expired(link)
    else:
        return False, f'shop ({shop}) not in list'
    if isinstance(is_expired, str):
        return False, is_expired
    elif is_expired:
        return False, "Link has expired or is invalid."
    else:
        return True, "Active"
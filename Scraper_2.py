'''
File: Scraper_2.py
Author: Ankit Yadav
Date: 20th July 2024
'''

'''
TO DO

Extract following information from the webpage 


Post ID: A unique identifier for each post.
Post Content: The text of the post.
Post Author: The author of the post.
Post Date: The date the post was made.
Post Category: Category or forum where the post was made.
Number of Comments: The total number of comments on the post.

From Comment 

Post ID: Link back to the original post.
Comment ID: A unique identifier for each comment.
Comment Content: Text of the comment.
Comment Author: Author of the comment.
Comment Date: Date the comment was posted. (the order of the comments is really important)
other meta data if available
'''

import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import os
import pickle
import re
import copy


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from tqdm import tqdm
# Setup Chrome WebDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)

import time

def format_text(text):
  text = text.strip()
  text = text.replace('\n', ' ')
  text = text.replace('\t', ' ')
  text = text.replace('\xa0', ' ')
  return text

def save_progress(progress_file, last_completed,post__dict,comment_dict):
    """Save the last completed index and heading to a file."""
    status_string = ""
    for i in last_completed:
        status_string = status_string + "|" + str(i)
    with open(progress_file, 'w') as f:
        f.write(str(status_string))
        f.close
    with open('data_post.pkl', 'wb') as f:
        pickle.dump(post__dict, f)
        f.close

    with open('data_comment.pkl', 'wb') as f:
        pickle.dump(comment_dict, f)
        f.close

def load_progress(progress_file):
    """Load the last completed index from a file."""
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            last_completed = str(f.read())
            last_completed = last_completed.split("|")
    else:
        return 0,0,0
    if os.path.exists('data_post.pkl') and os.path.exists('data_comment.pkl'):
        with open('data_post.pkl', 'rb') as f:
            post_dict = pickle.load(f)
        
        with open('data_comment.pkl', 'rb') as f:
            comment_dict = pickle.load(f)
    else:
        return 0,0,0
    return last_completed,post_dict,comment_dict  # Default to 0 if no progress file exists

def find_last_page(soup):
    script_tag = soup.find('script', string=re.compile(r'LITHIUM.Cache.CustomEvent.set'))

    if script_tag:
        script_content = script_tag.string
    
        page_numbers = re.findall(r'"page":(\d+)', script_content)
        page_numbers = list(map(int, page_numbers))  # Convert to integers
        
        max_page = max(page_numbers) if page_numbers else 1
        return max_page
    else:
        return 1
    
def check_length_of_metadata(post_author,post_time,post_date):
    if len(post_author) == len(post_time) == len(post_date):
        return True
    else:
        return False
    
def Check_Post_metadata(post_dict):
    if len(post_dict['Post_ID']) == len(post_dict['Post_Content']) == len(post_dict['Post_Author']) == len(post_dict['Post_Author_Rank']) == len(post_dict['Post_Date']) == len(post_dict['Post_Time']) == len(post_dict['Post_Category']) == len(post_dict['Number_of_Comments']) == len(post_dict['Post_URL']):
        return True
    else:
        return False
    
def Check_Comment_metadata(comment_dict):
    if len(comment_dict['Post_ID']) == len(comment_dict['Comment_ID']) == len(comment_dict['Comment_Content']) == len(comment_dict['Comment_Author']) == len(comment_dict['Comment_Date']) == len(comment_dict['Comment_Time']):
        return True
    else:
        return False

def scrape_anxiety_comments(heading_list = ['Post_data'],max_page=[16]):
    url_base = "https://forums.beyondblue.org.au/t5/long-term-support-over-the/bd-p/c1-sc3-b5"

    

    if os.path.exists('progress.txt') and os.path.exists('data_post.pkl'):
        last_completed,post_dict_cache,comment_dict_cache = load_progress('progress.txt')
        if last_completed == 0 and post_dict_cache == 0:
            return "Error in loading the progress. Please check the file."
        heading_list = last_completed[3]
        start_page = int(last_completed[2])
        max_page = last_completed[4]
        post_dict = post_dict_cache
        comment_dict = comment_dict_cache
        heading_list = [str(val.replace(' ','')) for val in heading_list.replace('[','').replace(']','').replace("'","").split(',')]
        max_page = [int(val) for val in max_page.replace('[','').replace(']','').split(',')]

        post_id_cnt = post_dict['Post_ID'][-1]
        comment_id_cnt = comment_dict['Comment_ID'][-1]


    else:

        start_page = 0

        post_id_cnt = 0
        comment_id_cnt = 0
        post_dict = {
        'Post_ID':[],
        'Post_Content':[],
        'Post_Author':[],
        'Post_Author_Rank':[],
        'Post_Date':[],
        'Post_Time':[],
        'Post_Category':[],
        'Number_of_Comments':[],
        'Post_URL':[],
        }
        comment_dict = {
        'Post_ID':[],
        'Comment_ID':[],
        'Comment_Content':[],
        'Comment_Author':[],
        'Comment_Date':[],
        'Comment_Time':[],
        }
    # Loading the progress


    for i,heading in enumerate(heading_list):
        head_key = heading.split('/')[0]

        pbar = tqdm(total=max_page[i])
        if start_page != 0:
            pbar.update(start_page)
        for page in range(start_page + 1,max_page[i]+1):
            url = url_base + '/' + heading + '/page/' + str(page)
            response = requests.get(url)

            #print("Processing Page:", url)
            post_url_list = []
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
            
                url_list = [tag.get('href', '') for tag in soup.find_all('a',attrs={'title': True})]
                post_url_list = [url for url in url_list[2:] if '/viewprofilepage/' not in url][1:]

                post_replies = soup.find_all('li', class_='custom-tile-replies')
                post_replies = [int(tag.find('b').text.strip()) for tag in post_replies][1:]

                post_author_rank = soup.find_all('div', class_='custom-tile-author-rank')
                post_author_rank = [tag.find('em').get_text() for tag in post_author_rank][1:]

                assert len(post_url_list) == len(post_replies), "Post count and reply count is not same."
                assert len(post_url_list) == len(post_author_rank), "Post count and post_author_rank count is not same."
                
                #####################################################################################
                # Storing the metadata to dictionaries
                post_dict['Post_ID'].extend([post_id_cnt + i for i in range(1,len(post_url_list) + 1)])
                post_id_cnt = post_id_cnt + len(post_url_list)

                post_dict['Post_Author_Rank'].extend(post_author_rank)
                post_dict['Number_of_Comments'].extend(post_replies)
                post_dict['Post_URL'].extend(post_url_list)
                #####################################################################################


                for pst_id,post_url in enumerate(post_url_list):
                    post_url = 'https://forums.beyondblue.org.au' + post_url


                    
                    try:
                        driver.get(post_url)
                        time.sleep(1) # For the page to load
                    except:
                        print("Failed to fetch the webpage. waiting for 30 seconds")
                        time.sleep(30) #30
                        service = Service(ChromeDriverManager().install())
                        driver = webdriver.Chrome(service=service)
                        driver.get(post_url)
                    
                    html_page = driver.page_source

                    if response.status_code == 200:
                        soup = BeautifulSoup(html_page, 'html.parser')
                        text_list = soup.find_all('div', class_="lia-message-body-content")
                        
                        # ###################################### Author Code ################################3 
                        post_author = soup.find('img', class_='lia-user-avatar-message').get('title')
                        post_time = soup.find('span', class_='local-time').get_text()
                        post_date = soup.find('span', class_='local-date').get_text()
                        # storing post the metadata
                        post_dict['Post_Content'].append(format_text(text_list[0].get_text()))
                        post_dict['Post_Author'].append(post_author)
                        post_dict['Post_Date'].append(post_date)
                        post_dict['Post_Time'].append(post_time)

                        # ##########################################################################################


                        last_page = find_last_page(soup) + 1
                        
                        # Bar for the comments 
                        pbar_comment = tqdm(total=last_page,leave=True)
                        
                        #last_page=2 # remove only for debugging
                        for comment_page in range(last_page):
                            if comment_page != 0:
                                comment_post_url = post_url + '/page/' + str(comment_page)
                                try:
                                    driver.get(comment_post_url)
                                    time.sleep(1) # For the page to load
                                except:
                                    print("Failed to fetch the webpage. waiting for 30 seconds")
                                    time.sleep(30) #30
                                    service = Service(ChromeDriverManager().install())
                                    driver = webdriver.Chrome(service=service)
                                    driver.get(comment_post_url)
                                
                                html_page = driver.page_source

                                if response.status_code == 200:
                                    soup = BeautifulSoup(html_page, 'html.parser')
                                    text_list = soup.find_all('div', class_="lia-message-body-content")
                                    ###################################### Author Code ################################3 
                                    post_comment_author = soup.find_all('img', class_='lia-user-avatar-message')
                                    post_comment_author = [a.get('title') for a in post_comment_author][1:-1]
                                    post_comment_time = soup.find_all('span', class_='local-time')
                                    post_comment_time = [a.get_text() for a in post_comment_time][1:]
                                    post_comment_date = soup.find_all('span', class_='local-date')
                                    post_comment_date = [a.get_text().replace("\u200e", "") for a in post_comment_date][1:]
                                    
                                    # in case of recent comments the autohr time is stored differently and we will search them now

                                    if len(post_comment_author) > len(post_comment_time):
                                        post_comment_latest_time = soup.find_all('span', class_='local-friendly-date')
                                        post_comment_latest_time = [a.get('title') for a in post_comment_latest_time]
                                        post_comment_time.extend([a.split(" ")[1] for a in post_comment_latest_time])
                                        post_comment_date.extend([a.split(" ")[0].replace("\u200e", "") for a in post_comment_latest_time])

                                    ######## Comment metadata   ########################
                                    comment_dict['Comment_ID'].extend([comment_id_cnt + i for i in range(1,len(text_list[1:])+1)])
                                    comment_id_cnt = comment_id_cnt + len(text_list[1:])
                                    comment_dict['Comment_Content'].extend([format_text(text.get_text()) for text in text_list[1:]])
                                    comment_dict['Comment_Author'].extend(post_comment_author)
                                    comment_dict['Comment_Date'].extend(post_comment_date)
                                    comment_dict['Comment_Time'].extend(post_comment_time)
                                    comment_dict['Post_ID'].extend([ post_dict['Post_ID'][pst_id] for i in range(1,len(text_list[1:])+1)])

                                    ##########################################################################################
                                    # Ensure the author count and comment count is same
                                    assert check_length_of_metadata(post_comment_author,post_comment_time,post_comment_date) is True, "Author count and comment count is not same."

                                else:
                                    print(" Comment not found.")
                                    return 0
                            if comment_page % 50 == 0:
                                time.sleep(30)
                            
                            pbar_comment.update(1)
                            pbar_comment.set_postfix({"Processing comment for": head_key,"Page No:": comment_page, "Total comments": len(text_list[1:])})
                        
                        
                        pbar_comment.close()
                        temp_dict = {'Posts':'','Comments':[]}
                        

                    else:
                        print(" Post not found.")
                        return None
                post_dict['Post_Category'].extend(['Long-term support over the journey' for i in range(1,len(post_url_list) + 1)])
                assert Check_Post_metadata(post_dict) is True, "Post metadata fileds dont have same length."
            else:
                print("Failed to fetch the webpage.")
                return None
            
            # saving progress
            confirmed_post = copy.deepcopy(post_dict)
            confirmed_comment = copy.deepcopy(comment_dict)
            save_progress('progress.txt', [i,page,heading_list,max_page],confirmed_post,confirmed_comment)
            pbar.update(1)
            pbar.set_postfix({"Processing for": head_key,"Page No:": page, "Total Posts": len(post_dict['Post_ID'])})

            # Storing at the Category level


            
            data_beyondblue_post_temp = pd.DataFrame(confirmed_post)
            data_beyondblue_post_temp.to_csv('post_tmp.csv', index=False)

            data_beyondblue_comment_temp = pd.DataFrame(confirmed_comment)
            data_beyondblue_comment_temp.to_csv('comment_tmp.csv', index=False)

            if page % 50 == 0:
                time.sleep(30)

        pbar.close()
        driver.quit()
        save_progress('progress.txt', [i,0,heading_list.remove(heading),max_page.remove(max_page[i])],confirmed_post,confirmed_comment)

    # Storing at the Overall level
    data_beyondblue_post = pd.DataFrame(confirmed_post)
    data_beyondblue_post.to_csv('post.csv', index=False)

    data_beyondblue_comment = pd.DataFrame(confirmed_comment)
    data_beyondblue_comment.to_csv('comment.csv', index=False)
    # return data_beyondblue
    return "Data Scraping Completed."

if __name__ == "__main__":
    heading_list = [ 'Post_data']
    
    max_page = [16]
    DataFrame = scrape_anxiety_comments(heading_list,max_page)
    #print(DataFrame.head())

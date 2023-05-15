# Stats 141XP
# 
# Bruinwalk Scraper
# 
# Dr. Sugano and Dr. Zhang Group 1
# 
# Author: Andrew Liu
# 
# Purpose: Scrape course data and reviews from bruinwalk.com. Perform sentiment analysis on review text.

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import math
from tqdm import tqdm
import pickle
from transformers import pipeline

###
# Function to scrape all courses and export as pkl
# @param None
# @return list of all courses
###
def scrape_all_courses():
    
    # base url for scraping classes
    base_url = 'https://www.bruinwalk.com/search/?category=classes'

    # iterate through all departments
    all_courses = []
    for dept_code in tqdm(range(300)):

        # get department url
        dept_code = str(dept_code + 1)
        dept_url = f'{base_url}&dept={dept_code}'

        # ping page and get response, pass to soup
        response = requests.get(dept_url)
        soup = BeautifulSoup(response.text, "html.parser")

        try:
            # iterate through all pages
            paginator = int(soup.find("div", class_="paginator").find_all("span")[1].get_text().replace('1 of ', ''))
            for page in range(paginator):

                # go to page and get response, pass to soup
                url = f"{dept_url}&page={page+1}"
                response = requests.get(url)
                soup = BeautifulSoup(response.text, "html.parser")

                # get all course urls and append to master list
                courses = soup.select('a[href^="/classes/"]')
                courses = list(set([i.get('href') for i in courses]))
                all_courses += courses
        except:
            pass

    # extract course codes
    all_courses = set([i.replace('/', '').replace('classes', '') for i in all_courses])

    # export as pkl
    with open('courses.pkl', 'wb') as file:
        pickle.dump(my_list, file)

    return all_courses

###
# Function to get all courses for a department, gets all courses if no department specified
# @param department code (optional)
# @return list of course codes
###
def get_courses(dept_code = None):
    
    # base url for scraping classes
    # if no department code then scrapes all classes across all departments otherwise specify department
    base_url = 'https://www.bruinwalk.com/search/?category=classes'
    if dept_code != None:
        base_url += f'&dept={dept_code}'
    
        # ping page and get response, pass to soup
        response = requests.get(base_url)
        soup = BeautifulSoup(response.text, "html.parser")

        # iterate through all pages
        all_courses = []
        paginator = int(soup.find("div", class_="paginator").find_all("span")[1].get_text().replace('1 of ', ''))
        for page in range(paginator):

            # go to page and get response, pass to soup
            url = f"{base_url}&page={page+1}"
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")

            # get all course urls and append to master list
            courses = soup.select('a[href^="/classes/"]')
            courses = list(set([i.get('href') for i in courses]))
            all_courses += courses

        # extract course codes
        all_courses = [i.replace('/', '').replace('classes', '') for i in all_courses]
        
    else:
        # reading course list from pkl
        with open('courses.pkl', 'rb') as file:
            all_courses = pickle.load(file)

    return all_courses

###
# Function to get all courses for a course
# @param course code
# @return list of professors
###
def get_professors(course_code):
    
    # base url for scraping and append course code
    base_url = "https://www.bruinwalk.com"
    course = course_code.replace('-', ' ').title()
    url = f"{base_url}/classes/{course_code}"
    
    # ping page and get response, pass to soup
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    
    # iterate through all pages
    all_professors = []
    paginator = int(soup.find("div", class_="paginator").find_all("span")[1].get_text().replace('1 of ', ''))
    for page in range(paginator):
        
        # go to page and get response, pass to soup
        url =f"{base_url}/classes/{course_code}?page={page+1}"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        
        # get all professor urls and append to master list 
        professors = soup.select('a[href^="/professors/"]')
        professors = list(set([i.get('href') for i in professors]))
        all_professors += professors
    
    return all_professors

###
# Function to get all reviews for a course
# @param course code
# @ return dataframe of course data and reviews
###
def scrape_reviews(course_code):
    
    # create dataframe
    col_names = ['Course Code', 'Course Name', 'Department', 'Professor', 'Course Ratings', 'Quarter', 'Year', 'Grade', 'Review Date', 'Review Text', 'Review Upvote', 'Review Downvote']
    df = pd.DataFrame(columns = col_names)
    idx = 0
    
    # base url for scraping
    base_url = "https://www.bruinwalk.com"
        
    # get professors
    professors = get_professors(course_code)
    
    # iterate through all professors
    for i in professors:
        try:
            # get professor name
            start = '/professors/'
            end = f'/{course_code}/'
            start_index = i.index(start) + len(start)
            end_index = i.index(end)
            prof = i[start_index:end_index].replace('-', ' ').title()

            # ping professor page and get response, pass to soup 
            url = f"{base_url}{i}"
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")

            # extract department name
            dep = soup.find("div", class_="department-name").get_text().strip().replace('Department of ', '')

            # extract course code
            course_c = soup.find("span", class_="aggregate-type-badge").get_text()

            # extract course name
            course_n = soup.find("div", class_="aggregate-header content-row").find('h2').get_text()

            # extract overall score and users
            overall_score = soup.find("div", class_="overall-score").get_text().replace(' ', '')
            replacements = ['\n', '\t', ' ', 'OverallRating', 'Basedon', 'Users', 'User']
            overall_users = soup.find("div", class_="overall-text").get_text()
            for j in replacements:
                overall_users = overall_users.replace(j, '')

            # extract specific ratings
            ratings = soup.find_all("div", class_="ind-rating")
            options = ['Easiness', 'Clarity', 'Workload', 'Helpfulness']
            course_ratings = {'Overall' : math.nan, 'Users': math.nan}
            if overall_score != 'N/A':
                course_ratings['Overall'] = float(overall_score)
            if overall_users != '':
                course_ratings['Users'] = float(overall_users)
            for j in options:
                course_ratings[j] = math.nan
            replacements = [' 5 ', '\n', ' ', '\t', '/']
            for j in ratings[:4]:
                val = j.find("span", class_="value").get_text()
                for k in replacements:
                    val = val.replace(k, '')
                for l in options:
                    if l in j.get_text():
                        if val != 'N/A':
                            course_ratings[l] = float(val)
            course_ratings = str(course_ratings)

            # iterate through all pages
            paginator = int(soup.find("div", class_="paginator").find_all("span")[1].get_text().replace('1 of ', ''))
            for page in range(paginator):

                # go to page and pass to soup
                url = f"{base_url}{i}?page={page+1}"
                response = requests.get(url)
                soup = BeautifulSoup(response.text, "html.parser")

                # extract reviews
                reviews = soup.find_all("div", class_="review reviewcard")

                # iterate through all reviews
                for j in reviews:

                    # extract quarter and grade element
                    quarter_and_grade = j.select('div[class^="row collapse"]')[0]
                    quarter_year = quarter_and_grade.select('div')[0].get_text()
                    grade = quarter_and_grade.select('div')[1].get_text()

                    # extract quarter and year
                    replacements = ['\n', ' ', 'Quarter:']
                    for k in replacements:
                        quarter_year = quarter_year.replace(k, '')
                    if quarter_year == 'N/A':
                        quarter = 'N/A'
                        year = 'N/A'
                    else:
                        quarter_year = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', quarter_year).split(' ')
                        quarter = quarter_year[0]
                        year = quarter_year[1]

                    # extract grade
                    replacements = ['\n', ' ', 'Grade:']
                    for k in replacements:
                        grade = grade.replace(k, '')

                    # extract review date
                    review_date = j.select('span[class^="date"]')[0].get_text()
                    replacements = ['\n', ' ']
                    for k in replacements:
                        review_date = review_date.replace(k, '')
                    if '.' in review_date:
                        index = review_date.index('.')
                        review_date = review_date[:index][:3] + review_date[index:]
                    # convert to datetime, standardize formatting
                    input_formats = ["%b.%d,%Y", "%B%d,%Y"]
                    for k in input_formats:
                        try:
                            review_date = datetime.strptime(review_date, k)
                            break
                        except:
                            pass
                    output_format = "%m/%d/%Y"
                    review_date = review_date.strftime(output_format)

                    # extract review text
                    review_text = j.find("div", class_="expand-area review-paragraph").get_text().replace('\n', '')

                    # extract review upvote value
                    review_upvote = int(j.find("span", class_="upvote-value").get_text())

                    # extract review downvote value
                    review_downvote = int(j.find("span", class_="downvote-value").get_text())

                    # append to dataframe and increment index
                    df.loc[idx] = [course_c, course_n, dep, prof, course_ratings, quarter, year, grade, review_date, review_text, review_upvote, review_downvote]
                    idx += 1
        except:
            pass
    
    #drop duplicates
    df = df.drop_duplicates(keep = 'first').reset_index(drop = True)
    return df

###
# Function to scrape all courses within a department, all courses if no department specified
# @param department code (optional)
# @return dataframe of course data and reviews
###
def scrape_courses(dept_code = None):
    
    try:
        # save progress
        df = pd.read_csv('progress.csv')
    except:
        # create dataframe
        col_names = ['Course Code', 'Course Name', 'Department', 'Professor', 'Course Ratings', 'Quarter', 'Year', 'Grade', 'Review Date', 'Review Text', 'Review Upvote', 'Review Downvote']
        df = pd.DataFrame(columns = col_names)
    
    # get courses
    courses = get_courses(dept_code)
    
    # iterate through all courses and scrape reviews
    for i in tqdm(range(len(df['Course Code'].unique()), len(courses))):
        df = pd.concat([df, scrape_reviews(courses[i])]).reset_index(drop = True)
        # save progress every 1000 in case need to restart
        if i % 1000 == 0:
            df.to_csv('progress.csv', index = False)
        
    # drop duplicates
    df = df.drop_duplicates(keep = 'first').reset_index(drop = True)
    return df

###
# Function to perform sentiment analysis on dataframe
# @param dataframe with review text
# @return dataframe with sentiment analysis
###
def sentiment_analysis(df):
    
    sentiment_analysis = pipeline("sentiment-analysis")

    # sentiment analysis on review text, label and score using hugging face
    for i in range(len(df)):

        text = df.at[i, 'Review Text']
        # if greater than character limit truncate down to max
        if len(text) > 512:
            text = text[:512]

        # perform sentiment analysis
        sentiment_results = sentiment_analysis(text)
        df.at[i, 'Review Sentiment Label'] = sentiment_results[0]["label"]
        df.at[i, 'Review Sentiment Score'] = sentiment_results[0]["score"]
        
    return df

# scrape for stats department, code is 176
df = scrape_courses('176')

# sentiment analysis
df = sentiment_analysis(df)

# export dataframe to csv
df.to_csv('bruinwalk_stats.csv', index = False)

# scrape for all departments and courses
df = scrape_courses()

# sentiment analysis
df = sentiment_analysis(df)

#export dataframe to csv
df.to_csv('bruinwalk.csv', index = False)


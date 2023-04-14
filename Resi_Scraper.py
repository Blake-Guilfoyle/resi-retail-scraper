from time import sleep
import requests
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import csv
import pandas as pd
import numpy as np
from openpyxl import Workbook
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException 
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import SessionNotCreatedException
from selenium.webdriver.chrome.options import Options
import json
import re
import random
from datetime import date
import sys
from twisted.internet import reactor
import scrapy
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scraper_api import ScraperAPIClient
import os
import logging
import ftplib
from shutil import copyfile

##ScraperAPI
client = ScraperAPIClient("a72d35bfd28f7f6e0bc0b3ccd2954d22")

#Variables
global output_dir
global date_today
global PlanDetails
global DemandPlanFormat
global List_Urls
global Scrapy_logger
global Driver_logger
global Driver_dir
global Stream_handler
global Retailer_Logos
global requestHeader

DemandPlanFormat = {'plans':[]}
List_Urls = []
PlanDetails = {}
date_today = str(date.today().strftime("%d-%m-%Y"))

#output_dir = 'Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/Residential/'
#output_dir = 'C:/Users/Blake/Desktop/A1CRM/EP/Web Scraper/Scrapes/Residential/'
#Driver_dir = 'C:/Users/Blake/Downloads/chromedriver.exe'

##Setting Logs files and config
#Driver_logger = logging.getLogger('Selenium')
#Driver_logger.setLevel(logging.DEBUG)
#formatter = logging.Formatter('%(asctime)s %(lineno)d %(levelname)s %(message)s') 
#Driver_file_handler = logging.FileHandler('Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/Selenium.log')
#Driver_file_handler = logging.FileHandler('C:/Users/Blake/Desktop/A1CRM/EP/Web Scraper/Scrapes/Selenium.log')
#Driver_file_handler = logging.FileHandler('Selenium.log')
#Driver_file_handler.setFormatter(formatter)
#Driver_logger.addHandler(Driver_file_handler)


#Scrapy_logger = logging.getLogger('Scrapy')
#Scrapy_logger.setLevel(logging.DEBUG)

#Scrapy_file_handler = logging.FileHandler('Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/Scrapy.log')
#crapy_file_handler = logging.FileHandler('C:/Users/Blake/Desktop/A1CRM/EP/Web Scraper/Scrapes/Scrapy.log')
#Scrapy_file_handler = logging.FileHandler('Scrapy.log')
#Scrapy_file_handler.setFormatter(formatter)
#Scrapy_logger.addHandler(Scrapy_file_handler)

##Strean handler for ouptuing to stdout
#Stream_handler = logging.StreamHandler()
#Stream_handler.setFormatter(formatter)
#Scrapy_logger.addHandler(Stream_handler)
#Driver_logger.addHandler(Stream_handler)




#Selenium Chrome options
chrome_options = Options()
chrome_options.add_argument("--incognito")
chrome_options.add_argument("--headless")

#Dictionaries:
DbByPostcode = {
    'Energex':4215,
    'Ergon':4350,
    'Ausgrid':2000,
    'Endeavour':2115,
    'Essential Energy':2339,
    'Evoenergy':2600,
    'SA Power Networks': 5000,
    'TasNetworks':7000}

DbListOfPlans = {
    'Energex':[],
    'Ergon':[],
    'Ausgrid':[],
    'Endeavour':[],
    'Essential Energy':[],
    'Evoenergy':[],
    'SA Power Networks':[],
    'TasNetworks':[]}

def save_dict(dictionary,name):
    """Function to save dictionary to file 
    Args:
        dictionary: The dictionary to be saved A predefined dictionary of a single postcode in each distribution network
        name (str): The name of the output file, default is str(dictionary)
        
        """
    def set_default(obj):
        if isinstance(obj, set):
            return list(obj)
        raise TypeError
    with open(output_dir+name+"_"+date_today+".json", 'w') as f: 
        json.dump(dictionary, default=set_default, sort_keys=True, fp = f )
    return('Successfully saved ' + name)
def generateAccessToken():
    clientId = "1000.U10YZGANQ0A8BOGQQ6BLR7DDMN4NUX"
    clientSecret = "f58e5fc154b1c7d055ae816d805a41734a53e9b27b"
    refreshToken = "1000.cdef1425fcca4809f02196950de010a5.95c05b4c25c266a33dbb63ad0c7bb19b"
    url2 = "https://accounts.zoho.com/oauth/v2/token?refresh_token="+refreshToken+"&client_id="+clientId+"&client_secret="+clientSecret+"&grant_type=refresh_token"
   
    resp = requests.post(url2)

    if resp.status_code != 200:
        print(resp.status_code)
        print(resp.json())
        accessToken = False      

    else:
        print(resp.status_code)
        data = resp.json()
        print(data)
        accessToken = data['access_token']
    return accessToken


accessToken = generateAccessToken()
if accessToken != False:
    requestHeader = {
        "Authorization": "Zoho-oauthtoken "+accessToken
    }


#resp = requests.get("https://www.zohoapis.com/workdrive/api/v1/teamfolders/c869z9e15982dcfc8451a8a24874564f74f14/files",headers=requestHeader)
#print(requestHeader.get("Authorization"))

def uploadFile(fileName, folderName,jsonObject):
    uploadUrl = "https://www.zohoapis.com/workdrive/api/v1/upload?"
    
    if folderName == "Base":
        folderId = "c869z9e15982dcfc8451a8a24874564f74f14"
    if folderName == "Commercial":
        folderId = "c869z4a2c7f854af047a48a01de2d04d22f42"
    if folderName == "Residential":
        folderId = "c869z4140e95a00e74fe2a4082249038cef3a"
    
    url = uploadUrl + "filename="+fileName+"&parent_id="+folderId+"&override-name-exist=True"
    payload = {}
    files =[ ('content',(fileName,json.dumps(jsonObject),'application/json'))]

    response = requests.request("POST",url,headers=requestHeader,data=payload,files=files)
    return response

#GETS PLAN LINKS TO FEED SCRAPY
def GetPlans(DbByPostcode,Type):

    """A Parent Function that Scrapes main results page on Energy Made Easy
    Args:
        DbByPostcode: A predefined dictionary of a single postcode in each distrubition network
        type: the type of premise to be scraped, either resi (Residential) or com (Commercial)

    Returns:
        dictionary (DbListOfPlans): links to each electricity plan by distributor
    """
    
    ##with open("Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/retailerLogos.json","r") as json_file:
    #with open("C:/Users/Blake/Desktop/A1CRM/EP/Web Scraper/Scrapes/retailerLogos.json","r") as json_file:
    global Retailer_Logos
    Retailer_Logos = {}

    try:
        #Driver_logger.info("Launching ChromeDriver")
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        #driver = webdriver.Chrome(Driver_dir, options=chrome_options)

    except SessionNotCreatedException:
        #Driver_logger.critical("ChromeDriver Needs to be updated",exc_info=True)
        print("ChromeDriver Needs to be updated")
        exit()

    #Driver_logger.info("ChromeDriver Launched")

    for key,value in DbByPostcode.items():
        #Driver_logger.info("Openining EnergyMadeEasy and getting plans for the {} network postcode {}".format(key,value))
        driver.get("https://www.energymadeeasy.gov.au/start")
        try:
            driver.find_element_by_name("electricity").click()
            driver.find_element_by_id("autocomplete-postcode").click()
            form = driver.find_element_by_id("autocomplete-postcode")
            form.send_keys(value)
            sleep(3)
            driver.find_element_by_class_name("autocomplete__results__item").click()
        except NoSuchElementException:
            #Driver_logger.critical("Couldn't complete first form section",exc_info=True)
            exit()
        
        ##Change of button options for Residential
        if Type == "Resi":
            #Completes Resi Form
            try:
                #Driver_logger.info("Completing Residential Form")
                
                driver.find_element_by_name("1Person").click()
                driver.find_element_by_name("noUsage").click()
                sleep(0.5)

                if len(driver.find_elements_by_name("cl-Yes")) > 0:
                    driver.find_element_by_name("cl-Yes").click()
                sleep(2)
                if len(driver.find_elements_by_name("ac-No")) > 0:
                    driver.find_element_by_name('ac-No').click()
                if len(driver.find_elements_by_name("ewh-No")) > 0:
                    driver.find_element_by_name('ewh-No').click()
                if len(driver.find_elements_by_name("uh-No")) > 0:
                    driver.find_element_by_name('uh-No').click()
                if len(driver.find_elements_by_name("esh-No")) > 0:
                    driver.find_element_by_name('esh-No').click()
                if len(driver.find_elements_by_name('pv-No')) > 0:
                    driver.find_element_by_name('pv-No').click()
                if len(driver.find_elements_by_name('sp-No')) > 0:
                    driver.find_element_by_name('sp-No').click()
                sleep(0.5)
                if len(driver.find_elements_by_name('underfloorHeating-No')) > 0:
                    driver.find_element_by_name('underfloorHeating-No').click()
                sleep(0.5)
                if len(driver.find_elements_by_name("gasMethod-Don't have gas")) > 0:
                    driver.find_element_by_name("gasMethod-Don't have gas").click()

                driver.find_element_by_name('smartMeter-Not sure').click()
                driver.find_element_by_name('electricity-retailer').click()
                driver.find_element_by_xpath("/html/body/div/div[2]/div/main/div/div/div/div[3]/div[3]/div/section[2]/fieldset/div/select/option[2]").click()
                #Driver_logger.info("Completed Residential Form")

            except NoSuchElementException:
                #Driver_logger.critical("Couldn't complete Residential Form",exc_info=True)
                exit()


        #Accept Terms and conditions:
        try:
            driver.find_element_by_class_name('image-checkbox__label').click()
            driver.find_element_by_class_name('btn.btn--green.btn--large.btn-compare-plans').click()
            #Driver_logger.info("Accepted Terms and Conditions")
        except NoSuchElementException:
            #Driver_logger.critical("Could not accept Terms and conditions",exc_info=True)
            exit()
        
        #Waiting for url Change to start scrape:
        while driver.current_url == 'https://www.energymadeeasy.gov.au/start':
            if driver.current_url == 'https://www.energymadeeasy.gov.au/results':
                break
        sleep(1)
        #Driver_logger.info("EnergyMadeEasy Results have loaded")
        sleep(1)
   
        if len(driver.find_elements_by_class_name('_hj-3g5tm__styles__closeModalBtn'))> 0:
            driver.find_element_by_class_name('_hj-3g5tm__styles__closeModalBtn').click()
        else:
            pass

        try:
            sleep(1)
            driver.find_element_by_class_name('main-filters-button-desktop').click()
            sleep(0.5)
            driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[1]/div[2]/div[2]/div[1]/div[6]/div[2]/div[1]/label").click()
            driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[1]/div[2]/div[2]/div[1]/div[6]/div[2]/div[3]/label").click()
            driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[1]/div[2]/div[2]/div[1]/div[6]/div[2]/div[3]/label").click()
            driver.find_element_by_id('applyFilters').click()
            driver.find_element_by_id('showAllPlans').click()
            #Driver_logger.info("Filterd for Plans")

        except NoSuchElementException:
            #Driver_logger.critical("Couldn't complete filters for plans",exc_info=True)
            exit()
        
        while len(driver.find_elements_by_class_name("show-more-button.btn")) > 0:
            driver.find_element_by_class_name("show-more-button.btn").click()
            
        NumOfPlans = len(driver.find_elements_by_class_name("plan-results-tile"))+ 1
        Plans = []
        for x in range(1,NumOfPlans):
            if len(driver.find_elements_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[4]/div["+str(x)+"]/div/div[2]/div[1]/a")) > 0:
                raw_link = driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[4]/div["+str(x)+"]/div/div[2]/div[1]/a").get_attribute("href")
                raw_logo = driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[4]/div["+str(x)+"]/div/div[1]/img").get_attribute("src")

                Plans.append(raw_link)

                raw_rcode = raw_link.replace('https://www.energymadeeasy.gov.au/plan?id=','')
                raw_rcode = raw_rcode[0:3]

                if raw_rcode not in Retailer_Logos.keys():
                    Retailer_Logos[raw_rcode] = raw_logo
                    #with open("Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/retailerLogos.json", 'w') as json_file: 
                    #with open("C:/Users/Blake/Desktop/A1CRM/EP/Web Scraper/Scrapes/retailerLogos.json", 'w') as json_file:
                        
                        #json.dump(Retailer_Logos,json_file)
                


            if len(driver.find_elements_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[3]/div["+str(x)+"]/div/div[2]/div[1]/a")) > 0:
                raw_link = driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[3]/div["+str(x)+"]/div/div[2]/div[1]/a").get_attribute("href")
                if len(driver.find_elements_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[4]/div["+str(x)+"]/div/div[1]/img")) > 0:
                    raw_logo = driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[4]/div["+str(x)+"]/div/div[1]/img").get_attribute("src")

                elif len(driver.find_elements_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[3]/div["+str(x)+"]/div/div[1]/img")) > 0:
                    raw_logo = driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[3]/div["+str(x)+"]/div/div[1]/img").get_attribute("src")
                    
                Plans.append(raw_link)

                raw_rcode = raw_link.replace('https://www.energymadeeasy.gov.au/plan?id=','')
                raw_rcode = raw_rcode[0:3]

                if raw_rcode not in Retailer_Logos.keys():
                    Retailer_Logos[raw_rcode] = raw_logo
                    #with open("Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/retailerLogos.json", 'w') as json_file: 
                    #with open("C:/Users/Blake/Desktop/A1CRM/EP/Web Scraper/Scrapes/retailerLogos.json", 'w') as json_file:
                        #json.dump(Retailer_Logos, json_file)
        
        #Driver_logger.info("Scraped List of Plan Links")
        #Searching for demand plans:
        try:
            driver.find_element_by_class_name('main-filters-button-desktop').click()
            sleep(1)
            #Demand Xpath
            driver.find_element_by_xpath("/html/body/div[1]/div[2]/div/main/section/div[1]/div/div[1]/div[2]/div[2]/div[1]/div[6]/div[2]/div[7]/label").click()
            sleep(1)
            driver.find_element_by_id('applyFilters').click()
            driver.find_element_by_id('showAllPlans').click()
            #Driver_logger.info("Filterd for Demand Plans")

        except NoSuchElementException:
            #Driver_logger.critical("Couldn't complete Demand filters for plans",exc_info=True)
            pass
        
        while len(driver.find_elements_by_class_name("show-more-button.btn")) > 0:
            driver.find_element_by_class_name("show-more-button.btn").click()
        
        NumOfPlans = len(driver.find_elements_by_class_name("plan-results-tile")) + 1
        for x in range(1,NumOfPlans):
            if len(driver.find_elements_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[4]/div["+str(x)+"]/div/div[2]/div[1]/a")) > 0:
                raw_link = driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[4]/div["+str(x)+"]/div/div[2]/div[1]/a").get_attribute("href")
                raw_logo = driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[4]/div["+str(x)+"]/div/div[1]/img").get_attribute("src")

                Plans.append(raw_link)

                raw_rcode = raw_link.replace('https://www.energymadeeasy.gov.au/plan?id=','')
                raw_rcode = raw_rcode[0:3]

                if raw_rcode not in Retailer_Logos.keys():
                    Retailer_Logos[raw_rcode] = raw_logo
                    #with open("Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/retailerLogos.json", 'w') as json_file:
                    #with open("C:/Users/Blake/Desktop/A1CRM/EP/Web Scraper/Scrapes/retailerLogos.json", 'w') as json_file:
                        #json.dump(Retailer_Logos,json_file)
                


            if len(driver.find_elements_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[3]/div["+str(x)+"]/div/div[2]/div[1]/a")) > 0:
                raw_link = driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[3]/div["+str(x)+"]/div/div[2]/div[1]/a").get_attribute("href")
                if len(driver.find_elements_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[4]/div["+str(x)+"]/div/div[1]/img")) > 0:
                    raw_logo = driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[4]/div["+str(x)+"]/div/div[1]/img").get_attribute("src")

                elif len(driver.find_elements_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[3]/div["+str(x)+"]/div/div[1]/img")) > 0:
                    raw_logo = driver.find_element_by_xpath("/html/body/div/div[2]/div/main/section/div[1]/div/div[3]/div[3]/div["+str(x)+"]/div/div[1]/img").get_attribute("src")
                    
                Plans.append(raw_link)

                raw_rcode = raw_link.replace('https://www.energymadeeasy.gov.au/plan?id=','')
                raw_rcode = raw_rcode[0:3]

                if raw_rcode not in Retailer_Logos.keys():
                    Retailer_Logos[raw_rcode] = raw_logo
                    #with open("Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/retailerLogos.json", 'w') as json_file: 
                    #with open("C:/Users/Blake/Desktop/A1CRM/EP/Web Scraper/Scrapes/retailerLogos.json", 'w') as json_file:
                        #json.dump(Retailer_Logos, json_file)

        RawRetailerLinks = Plans
        DbListOfPlans[key] = RawRetailerLinks 

    driver.close()
    #Driver_logger.info("Driver closed")
    return(DbListOfPlans)

#if os.path.isfile(output_dir+"ResiDbListOfPlans"+"_"+date_today+".json") == True:
#    f = open(output_dir+"ResiDbListOfPlans"+"_"+date_today+".json") 
#    DbListOfPlans = json.load(f)
#    Driver_logger.info('ResiDbListOfPlans from {}, already exists using that file'.format(date_today))

#else:
DbListOfPlans = GetPlans(DbByPostcode,"Resi")
fileName = "ResiDbListOfPlans" + date_today + ".json"
uploadFile(fileName, "Residential",DbListOfPlans)
uploadFile("Resi_retailerLogos.json","Base",Retailer_Logos)
#save_dict(DbListOfPlans,"ResiDbListOfPlans")


for key,value in DbListOfPlans.items():

    for raw_url in value:
        raw_url = raw_url.replace("https://www.energymadeeasy.gov.au/plan?id=","https://api.energymadeeasy.gov.au/plans/dpids/")
        url = raw_url.replace("&","?")
        List_Urls.append(url)

print("Changed Format")
#Driver_logger.info("Changed format of urls to api url")

#Spider to get plan details
class EnergyMadeEasySpider(scrapy.Spider):
    
    name ='EnergyMadeEasyScraper'
    global List_Urls
    global PlanDetails
    global Scrapy_logger 

    headers = {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
            "origin": "https://www.energymadeeasy.gov.au",
            "referer": "https://www.energymadeeasy.gov.au/start",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36"
        }
    def generateAccessToken():
        clientId = "1000.U10YZGANQ0A8BOGQQ6BLR7DDMN4NUX"
        clientSecret = "f58e5fc154b1c7d055ae816d805a41734a53e9b27b"
        refreshToken = "1000.cdef1425fcca4809f02196950de010a5.95c05b4c25c266a33dbb63ad0c7bb19b"
        url2 = "https://accounts.zoho.com/oauth/v2/token?refresh_token="+refreshToken+"&client_id="+clientId+"&client_secret="+clientSecret+"&grant_type=refresh_token"
    
        resp = requests.post(url2)

        if resp.status_code != 200:
            print(resp.status_code)
            print(resp.json())
            accessToken = False      

        else:
            print(resp.status_code)
            data = resp.json()
            print(data)
            accessToken = data['access_token']
        return accessToken

    global accessToken
    accessToken = generateAccessToken()
    if accessToken != False:
        requestHeader = {
            "Authorization": "Zoho-oauthtoken "+accessToken
        }


    #resp = requests.get("https://www.zohoapis.com/workdrive/api/v1/teamfolders/c869z9e15982dcfc8451a8a24874564f74f14/files",headers=requestHeader)
    #print(requestHeader.get("Authorization"))

    def uploadFile(fileName, folderName,jsonObject):
        uploadUrl = "https://www.zohoapis.com/workdrive/api/v1/upload?"
        
        if folderName == "Base":
            folderId = "c869z9e15982dcfc8451a8a24874564f74f14"
        if folderName == "Commercial":
            folderId = "c869z4a2c7f854af047a48a01de2d04d22f42"
        if folderName == "Residential":
            folderId = "c869z4140e95a00e74fe2a4082249038cef3a"
        
        url = uploadUrl + "filename="+fileName+"&parent_id="+folderId+"&override-name-exist=True"
        payload = {}
        files =[ ('content',(fileName,json.dumps(jsonObject),'application/json'))]

        response = requests.request("POST",url,headers=requestHeader,data=payload,files=files)
        return response


    def start_requests(self):
        for url in List_Urls:
            #yield scrapy.Request(url=url,callback=self.parse_api,headers= self.headers,dont_filter=True)
            yield scrapy.Request(client.scrapyGet(url = url),callback=self.parse_api,headers= self.headers,dont_filter=True)

    def parse_api(self,response):
        response_url = response.url
        status_code = response.status
        if status_code == 404:
            print('404 Page Not Found, Called: {}'.format(response_url)) 
        else: pass
        print('STATUS CODE: {}'.format(status_code))
        raw_data = response.body
        plan = json.loads(raw_data)
        Company = plan[0]["planData"]["retailerName"].rstrip()
        if "postcode" in plan[0].keys():
            postcode = plan[0]['postcode']
        else:
            postcode = False


        if "distributor" in plan[0]["planData"].keys():
            DB = plan[0]["planData"]["distributor"]
        elif "supplyArea" in plan[0]["planData"].keys():
            DB = plan[0]["planData"]["supplyArea"][0]["name"]
        else: DB = False    

        Plan_Name = plan[0]["planData"]["planName"]
        Plan_ID = plan[0]["planData"]["planId"]
        contract = plan[0]["planData"]['contract'][0]

        #FEES SORTING
        if "fee" in contract.keys():

            for item in contract["fee"]:
                if "A membership fee" in item["description"]:
                    Membership_Fee = item["amount"]
                    break
                else: Membership_Fee = False
        else: False
        #SOLAR FIT SORTING
        Solar_FiT = {}
        if "solarFit" in contract.keys():
            for item in contract["solarFit"]:
                if item['type'] == "G" or item['rate'] > 42:
                    fit = contract["solarFit"]
                    a = fit.index(item)
                    del fit[a]

            if len(contract["solarFit"]) > 1:
                desclist = []
                Solar_FiT = {"Volume":[],"Inverter":[]}

                for item in contract["solarFit"]:
                    desc = item['description']
                    desc = desc.lower()
                    desclist.append(desc)
                    if re.search('first (.*) kwh',desc) != None:
                        Volume = False
                        Rate = False
                        Volume = re.search('first (.*) kwh',desc)
                        Volume = Volume.group(1)
                        Volume = Volume.strip()
                        Rate = round(item['rate'] / 100,5)
            
                        Solar_FiT["Volume"].append({"Rate":Rate,"Max":Volume})

                    if re.search('applies to remaining kwh',desc) != None:
                        Volume = False
                        Rate = False
                        Rate = round(item['rate'] / 100,5)
                        Solar_FiT["Volume"].append({"Rate":Rate,"Max":Volume})

                    if re.search("systems with (.*)kw or less",desc) != None:
                        Max = False
                        Min = False
                        Rate = False
                        Max = re.search("systems with (.*)kw or less",desc)
                        Max = Max.group(1)
                        Max = Max.strip()
                        Min = 0
                        Rate = round(item['rate'] / 100,5)
                        Solar_FiT["Inverter"].append({"Rate":Rate,"Min":Min,"Max":Max})

                    
                    if re.search("offers (.*) cents per kwh exported, this amount is gst exempt",desc) != None:
                        Rate = False
                        Rate = round(item['rate'] / 100,5)
                        Solar_FiT["Volume"].append({"Rate":Rate,"Max":False})
                    
                    if re.search("offers (.*) cents per kwh exported, subject to eligibility.",desc) != None:
                        Rate = False
                        Rate = round(item['rate'] / 100,5)
                        Solar_FiT["Volume"].append({"Rate":Rate,"Max":False})

                    if re.search('capacity between (.*)kw and (.*)kw',desc) != None:
                        CRange = re.search('capacity between (.*)kw and (.*)kw',desc)
                        Min = False
                        Max = False
                        Rate = False
                        Min = CRange.group(1).strip()
                        Max = CRange.group(2).strip()
                        Rate = round(item['rate'] / 100,5)
                        
                        Solar_FiT["Inverter"].append({"Rate":Rate,"Min":Min,"Max":Max})

                    if re.search('capacity over (.*)kw',desc) != None:
                        CRange = re.search('capacity over (.*)kw',desc)
                        Min = False
                        Max = False
                        Min = CRange.group(1).strip()
                        Rate = round(item['rate'] / 100,5)
                        
                        Solar_FiT["Inverter"].append({"Rate":Rate,"Min":Min,"Max":Max})



            if len(contract["solarFit"]) == 1:
                Rate = False
                Max = False
                contract["solarFit"][0]['description']
                desc = contract["solarFit"][0]['description'].lower()
                Solar_FiT = {"Volume":[],"Inverter":[]}
                Rate = round(contract["solarFit"][0]['rate'] /100,5)
                Solar_FiT["Volume"].append({'Rate':Rate,"Max":False})

                if re.search('first (.*) kwh',desc) != None:
                    Volume = False
                    Volume = re.search('first (.*) kwh',desc)
                    Volume = Volume.group(1)
                    Volume = Volume.strip()


                    Solar_FiT["Volume"][0]['Max'] = Volume

                if re.search('applies to remaining kwh',desc) != None:
                    Volume = False
                    Solar_FiT["Volume"][0]['Max'] = Volume

                if re.search("systems with (.*)kw or less",desc) != None:
                    Max = False
                    Min = False
                    Max = re.search("systems with (.*)kw or less",desc)
                    Max = Max.group(1)
                    Max = Max.strip()
                    Min = 0
                    Solar_FiT["Inverter"].append({"Rate":Rate,"Min":Min,"Max":Max})

                    
                if re.search("offers (.*) cents per kwh exported, this amount is gst exempt",desc) != None:
                    Volume = False
                    Solar_FiT["Volume"][0]['Max'] = Volume
                    
                if re.search("offers (.*) cents per kwh exported, subject to eligibility.",desc) != None:
                    Volume = False
                    Solar_FiT["Volume"][0]['Max'] = Volume

                if re.search('capacity between (.*)kw and (.*)kw',desc) != None:
                    CRange = re.search('capacity between (.*)kw and (.*)kw',desc)
                    Min = False
                    Max = False
                    Min = CRange.group(1).strip()
                    Max = CRange.group(2).strip()
                        
                    Solar_FiT["Inverter"].append({"Rate":Rate,"Min":Min,"Max":Max})

                if re.search('capacity over (.*)kw',desc) != None:
                    CRange = re.search('capacity over (.*)kw',desc)
                    Min = False
                    Max = False
                    Min = CRange.group(1).strip()

                    Solar_FiT["Inverter"].append({"Rate":Rate,"Min":Min,"Max":Max})
                invitationOnly = False
                if "eligibilityRestriction" in contract:
                    for item in contract["eligibilityRestriction"]:
                        if 'description' in item.keys():
                            desc = item['description']
                            desc = desc.lower()
                            if re.search('maximum (.*)kw inverter',desc) != None :
                                Max = False
                                Min = False
                                Max = re.search('maximum (.*)kw inverter',desc)
                                Max = Max.group(1)
                                Max = Max.strip()
                                Solar_FiT["Inverter"].append({"Rate":Rate,"Min":Min,"Max":Max})

                            if re.search('solar system no larger than (.*)kw',desc) != None :
                                Max = False
                                Min = False
                                Max = re.search('solar system no larger than (.*)kw',desc)
                                Max = Max.group(1)
                                Max = Max.strip()
                                Solar_FiT["Inverter"].append({"Rate":Rate,"Min":Min,"Max":Max})
                                    
                            if re.search('system is (.*)kw or less',desc) != None:
                                Max = False
                                Min = False
                                Max = re.search('system is (.*)kw or less',desc)
                                Max = Max.group(1)
                                Max = Max.strip()
                                Solar_FiT["Inverter"].append({"Rate":Rate,"Min":Min,"Max":Max})

                            if "invitation" in desc:
                                invitationOnly = True



            else: Solar_FiT = False
        #DEMAND SORTING
        
        if 'tariffPeriod' in contract.keys():
            if len(contract['tariffPeriod']) > 0:
                if 'demandCharge' in contract['tariffPeriod'][0].keys():
                        rawDplanDetails = contract['tariffPeriod'][0]
                        dPlan = {'tariffPeriod':[rawDplanDetails]}
                        DemandPlanFormat['plans'].append(dPlan)

                        D_Type = False
                        Rate = False
                        rawDemand = contract['tariffPeriod'][0]['demandCharge'][0]
                        Rate = round(rawDemand['rate']/100 * 1.1,5)
                        if 'description' in rawDemand.keys():
                            Desc = rawDemand['description'].lower()
                            if re.search("kw",Desc) != None:
                                D_Type = re.search("kw",Desc)
                                D_Type = "kW"

                            
                            if  re.search("kva",Desc) != None:
                                D_Type = re.search("kva",Desc)
                                D_Type = "kVA"

                        
                        if D_Type == False:
                            if 'name' in rawDemand.keys():
                                Name = rawDemand['name'].lower()
                                if re.search("kw",Name) != None:
                                    D_Type = re.search("kw",Name)
                                    D_Type = "kW"
                                
                                if  re.search("kva",Name) != None:
                                    D_Type = re.search("kva",Name)
                                    D_Type = "kVA"
                        

                        

                        if D_Type == False and Rate == False:
                            Demand_Charge = False
                            
                        elif D_Type == False and Rate != False:
                            Demand_Charge = {"Rate":Rate,"Type":"kW"}

                        elif D_Type != False and Rate != False:
                            Demand_Charge = {"Rate":Rate,"Type":D_Type}
                        
                        elif D_Type != False and Rate == False:
                            Demand_Charge = {"Rate":False,"Type":D_Type}



                else: Demand_Charge = False

        #DISCOUNT SORTING
        if "discount" in contract.keys():
            if "discountPercent" in contract["discount"][0].keys():
                Discount_Type = "Percent"
                Discount_Value = contract["discount"][0]["discountPercent"]
                Discount_Name = str(Discount_Value)+"% off of your usage and supply charges"
            elif "discountAmount" in contract["discount"][0].keys():
                Discount_Type = "Annual Amount"
                Discount_Name = contract["discount"][0]["name"]
                Discount_Value = contract["discount"][0]["discountAmount"]

            Discount = {"Value":Discount_Value,"Type":Discount_Type,"Name":Discount_Name}
        else: 
            Discount = False
            Discount_Value = False
            Discount_Type = False
            Discount_Name = False
        #CL SORTING
        if "controlledLoad" in contract.keys():
            if len(contract["controlledLoad"]) > 1:
                CL1_Rate = contract["controlledLoad"][0]["blockRate"][0]["unitPrice"] / 100
                CL1_Rate = round(CL1_Rate* 1.1,5)
                if "dailyCharge" in contract["controlledLoad"][0].keys():
                    CL1_Supply = contract["controlledLoad"][0]["dailyCharge"] / 100 
                    CL1_Supply = round(CL1_Supply* 1.1,5)
                    if CL1_Supply == 0:
                        CL1_Supply = False

                else: CL1_Supply = False

                if "unitPrice" in contract["controlledLoad"][1]["blockRate"][0].keys():
                    CL2_Rate = contract["controlledLoad"][1]["blockRate"][0]["unitPrice"]/ 100
                    CL2_Rate = round(CL2_Rate* 1.1,5)
                else: CL2_Rate = False

                if "dailyCharge" in contract["controlledLoad"][1].keys():
                    CL2_Supply = contract["controlledLoad"][1]["dailyCharge"]/ 100
                    CL2_Supply = round(CL2_Supply* 1.1,5)
                else: CL2_Supply = False

                if CL2_Supply == CL1_Supply:
                    CL2_Supply = False
            else:
                CL1_Rate = contract["controlledLoad"][0]["blockRate"][0]["unitPrice"]/ 100
                CL1_Rate = round(CL1_Rate* 1.1,5)
                CL2_Supply = False
                CL2_Rate = False
                if "dailyCharge" in contract["controlledLoad"][0].keys():
                    CL1_Supply = contract["controlledLoad"][0]["dailyCharge"]/ 100
                    CL1_Supply = round(CL1_Supply* 1.1,5)
                else: CL1_Supply = False

            Controlled_Load = {"CL1_Rate":CL1_Rate,"CL1_Supply":CL1_Supply,"CL2_Rate":CL2_Rate,"CL2_Supply":CL2_Supply}
        else: 
            Controlled_Load = False
        #TOU SORTING
        if "blockRate" in plan[0]["planData"]['contract'][0]["tariffPeriod"][0].keys():
            Usage_Rate = plan[0]["planData"]['contract'][0]["tariffPeriod"][0]["blockRate"][0]["unitPrice"]/ 100
            Usage_Rate = round(Usage_Rate* 1.1,5)
        else:
            Usage_Rate = False
        
        if "eligibilityRestriction" in contract.keys():
            Eligibility = []
            for item in contract["eligibilityRestriction"]:
                Eligibility.append(item)

        else: Eligibility = False
        #BENEFIT PERIOD
        if "benefitPeriod" in contract:
            Benefit_Period = contract["benefitPeriod"]

        else: Benefit_Period = False

        if 'onExpiry' in contract.keys():
            Expiry = contract["onExpiry"]
        else: Expiry = False

        if "variation" in contract.keys():
            Variation = contract["variation"]
        else: Variation = False

        if "terms" in contract.keys():
            Terms = contract["terms"]
            if "\u00a0" in Terms:
                Terms = Terms.replace("\u00a0"," ")

            if "\ufffd" in Terms:
                Terms = Terms.replace("\ufffd"," ")

        else: Terms = False

        if DB == "ERGON ENERGY":
            DB = "Ergon"

        if DB == "Endeavour" or DB == "Endeavour Energy":
            DB = "Endeavour_Energy"

        if DB == "Essential Energy" or DB == "Essential Energy - LNSP" or DB == "Essential Energy Far West" or DB == "Essential Energy Standard":
            DB = "Essential_Energy"

        if DB == "Evoenergy Electricity" or DB == "Evoenergy Gas":
            DB = "Evoenergy"

        if DB == "SA Power Networks" or DB == "SAPN":
            DB = "SA_Power_Networks"


        Supply_Charge = contract["tariffPeriod"][0]["dailySupplyCharge"]/ 100
        Supply_Charge = round(Supply_Charge* 1.1,5)

        if "additionalFeeInformation" in contract.keys():
            Additional_Fee_Information = contract["additionalFeeInformation"]
            if "\u00a0" in Additional_Fee_Information:
                Additional_Fee_Information = Additional_Fee_Information.replace("\u00a0"," ")
        else: Additional_Fee_Information = False
        
        if "effectiveDate" in plan[0]["planData"].keys():
            Effective_Date = plan[0]["planData"]["effectiveDate"]
        else: Effective_Date = False

        

        if "retailerCode" in plan[0]["planData"].keys():
            Retailer_Code = plan[0]["planData"]["retailerCode"]
        else: Retailer_Code = False
        ##C:/Users/Blake/Documents/Energy Partners NAS/
        ## with open("Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/retailerLogos.json","r") as json_file:
        #with open("C:/Users/Blake/Desktop/A1CRM/EP/Web Scraper/Scrapes/retailerLogos.json","r") as json_file:
        #    newLogoDict = json.load(json_file)
        #    if Retailer_Code in newLogoDict.keys():
        #        Logo = newLogoDict[Retailer_Code]
        #
        #        if Company not in newLogoDict.keys():
        #            newLogoDict[Company] = Logo
        #            #with open("Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/retailerLogos.json","w") as json_file:
        #            with open("C:/Users/Blake/Desktop/A1CRM/EP/Web Scraper/Scrapes/retailerLogos.json","w") as json_file:
                        
        #                json.dump(newLogoDict,json_file)
        #        else:pass    

        #    else: Logo = False
        Logo = False
        if DB not in PlanDetails.keys():
            PlanDetails[DB] = {}            

        if Company not in PlanDetails[DB].keys():
            PlanDetails[DB][Company] = []
            
        
        duplicate = False
        if len(PlanDetails[DB][Company]) > 0:
            for plan in PlanDetails[DB][Company]:
                pUsage_Rate = plan['Usage_Rate']
                pSupply_Charge = plan['Supply_Charge']
                pCL = plan["Controlled_Load"]
                pSolar_FiT = plan["Solar_FiT"]

                if Supply_Charge == pSupply_Charge and Usage_Rate == pUsage_Rate:
                    if pCL == Controlled_Load and pSolar_FiT == Solar_FiT:
                        duplicate = True

        lowerPlan_Name = Plan_Name.lower()        
        matches = ["sonnen","subscription","sonnenflat","subscription","gee"]

        if not any(x in lowerPlan_Name for x in matches) and duplicate == False and invitationOnly == False:
            PlanDetails[DB][Company].append({
                                        "Plan_Name":Plan_Name,
                                        "Plan_ID":Plan_ID,
                                        "Logo":Logo,
                                        "Pricing_Model":contract["pricingModel"],
                                        "Membership_Fee": Membership_Fee, 
                                        "Discount": Discount,
                                        "Usage_Rate": Usage_Rate,
                                        "Supply_Charge": Supply_Charge,
                                        "Solar_FiT": Solar_FiT,
                                        "Demand": Demand_Charge,
                                        "Controlled_Load": Controlled_Load,                                
                                        "Payment_Options":contract["paymentOption"],
                                        "GreenPower": "greenCharge" in contract.keys(),
                                        "Eligibility":Eligibility,
                                        "Inverter_Limit": False,
                                        "Benefit_Period":Benefit_Period,
                                        "Expiry":Expiry,
                                        "Terms":Terms,
                                        "Variation":Variation,
                                        "Additional_Fee_Information":Additional_Fee_Information,
                                        "Effective_Date":Effective_Date,
                                        "Retailer_Code":Retailer_Code,
                                        })

        #save_dict(PlanDetails,"ResiPlanDetails")
        fileName = "ResiPlanDetails_" + date_today + ".json"
        uploadFile(fileName, "Residential",PlanDetails)
        fileName = "DemandPlansFormat_" + date_today + ".json"
        uploadFile(fileName, "Residential",DemandPlanFormat)
        #save_dict(DemandPlanFormat,"DemandPlansFormat")
        yield(print("SCRAPED -- Retailer: {}, Plane_Name: {}, Plan_ID: {}".format(Company,Plan_Name,Plan_ID)))



##Running EnergyMadeEasySpide Class
runner = CrawlerRunner()
d = runner.crawl(EnergyMadeEasySpider)
d.addBoth(lambda _: reactor.stop())
reactor.run()


#Sending Files to Server
#C:/Users/Blake/Documents/Energy Partners NAS/
#session = ftplib.FTP_TLS('35.240.229.7','andrew@energypartners.com.au','6^I+zvO%Bks4')

#PlanPath = "Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/Residential/ResiPlanDetails_"+ date_today+".json"
#copyfile(PlanPath, "Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/temp/ResiPlanDetails.json")
#filename = "ResiPlanDetails.json"

#file = open("Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/temp/ResiPlanDetails.json",'rb')
#session.cwd("/json")                
#session.storbinary('STOR '+filename, file) 
#file.close()

#retailerLogoPath = "Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/retailerLogos.json"
#copyfile(retailerLogoPath, "Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/temp/retailerLogos.json")
#logosfilename = "retailerLogos.json"
#file = open("Z:/03_Marketing/Website/Retailer Rates Comparison/Scrapes/temp/retailerLogos.json",'rb')
#session.cwd("/json")                
#session.storbinary('STOR '+logosfilename, file) 
#file.close()

#session.quit()
#Site Ground SSH Access
#{"Hostname": "ssh.energypartners.com.au","Username": "u2-jfrn1la84pej","Password": "EPartners2806!","Port": 18765}
exit()



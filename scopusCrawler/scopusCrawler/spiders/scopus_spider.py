import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from scrapy.http import HtmlResponse
from scrapy.utils.project import get_project_settings
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium import webdriver
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary

class ScopusSpider(scrapy.Spider):
    name = 'scopus'

    def start_requests(self):
        useragent = "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 XL Build/OPD1.170816.004) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Mobile Safari/537.36"

        profile = webdriver.FirefoxProfile()
        profile.set_preference("general.useragent.override", useragent)
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')
        options.set_preference("dom.webnotifications.serviceworker.enabled", False)
        options.set_preference("dom.webnotifications.enabled", False)
        driver = webdriver.Firefox(firefox_profile=profile,options=options)
 
        url = 'https://www.scopus.com/record/display.uri?eid=2-s2.0-85131772089&origin=inward&txGid=d91aa6af05760f252f2e3c4f976bcf1c&retries=1'
        driver.get(url)
        with open('page.html', 'w') as f:
            f.write(driver.page_source)
        yield 

    def parse(self, response):
        pass
        #driver = response.request.meta['driver']
        #button = driver.find_element_by_css_selector( 'els-button.hydrated')
        #button.click()
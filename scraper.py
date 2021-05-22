from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options
import time
import datetime
import csv
import os


start_url = "https://www.expansion.com/mercados/cotizaciones/indices/ibex35_I.IB.html"

###############################################################
#   ->
#   stonks()
#   ->
#   current_stonks: csv
###############################################################
def stonks():
    options = Options()
    options.headless = True

    with webdriver.Firefox(options=options) as driver:
        wait = WebDriverWait(driver, 10)
        driver.get(start_url)

        today = str(datetime.datetime.today())
        year = today[:4]
        month = today[5:7]
        day = today[8:10]
        hour = today[11:13]
        minute = today[14:16]

        if minute != "30" and hour not in ['9','10','11','12','13','14','15','16','17','18']:
            return

        date = year + "_" + month + "_" + day + "_" + hour + "_" + minute

        # data from all session
        total_values = []
        reduced_values = []

        time.sleep(1)

        try:
            #recuperamos listado de acciones
            commodities = driver.find_elements_by_xpath("/html/body/main/section/div/div/div/ul/li/div/section/div/article/section[2]/ul[2]/li[1]/div/section/table/tbody/tr")

            for stock in commodities:
                new_row = []
                #recuperamos la información de cada acción
                values = stock.find_elements_by_xpath("td")        
                list_of_values = [x.text for x in values]
                for value in range(len(list_of_values)-1):
                    if value != (len(list_of_values)-2):
                        #eliminamos el punto de millar y cambiamos la coma decimal
                        v = list_of_values[value].replace('.','')
                        v = v.replace(',','.')
                        #print(v,end=",")
                        # fill new row
                        new_row.append(v)
                    else:
                        #print(list_of_values[value])
                        # fill new row
                        new_row.append(list_of_values[value])
                

                total_values.append(new_row)
                reduced_values.append( [new_row[0], new_row[1], new_row[5], new_row[6], new_row[9]] )

            #print(date + ".csv")
            with open(("stonks/all_" + date + ".csv"), 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(total_values)

            with open(("stonks/reduced_" + date + ".csv"), 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(reduced_values)

            # # add files to git repo
            os.system("git add" + " stonks/all_" + date + ".csv")
            os.system("git add" + " stonks/reduced_" + date + ".csv")

            driver.quit()
        except:
            driver.quit()
###############################################################

hours = [9,10,11,12,13,14,15,16,17,18]
minute = 30

stonks()
while True:
    hour_now = str(datetime.datetime.today()).split(" ")[1].split(":")
    if int(hour_now[0]) in hours and int(hour_now[1]) == minute:
        stonks()
        os.system('git add -A && git commit -m "' + str(datetime.datetime.today())[:10] + '"')
        os.system("git push")
        if int(hour_now[0]) == 18 and int(hour_now[1]) == minute:
            break

# os.system('git add -A && git commit -m "' + str(datetime.datetime.today())[:10] + '"')
# os.system("git push")


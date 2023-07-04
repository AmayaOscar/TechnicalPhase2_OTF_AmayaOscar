import requests
import json
import pandas as pd
import numpy as np
from csv import reader

def ContactCollection(url, token):                                                          #Function to make contact collection
    '''
    Description: Function to collect the records saved on the Hubspot account given in the document, with postman and the
    documentation we know the format of headers, body, and filters. These are the steps in a summarized way that the function follows:
        1. Establish the URL and the headers with the token and content-type.
        2. Enter into a while that no ends until a bool flag ("flagEndRecords") becomes True.
        3. The data is extracted in 100 groups, every time that we extract a group, the limit and after variables changes to take
           the next group.
        4. The group extracted from hubspot gets saved on the recordList list.
        5. Return recordList that contains a list with all the contacts extracted.

    Arguments:
        1. url: The URL of the List API.
        2. token: The token for the contacts to extract.

    Returns:
        1. recordsList: A list with the information extracted.
    '''

    curl = url                                                                              #URL to obtain a complete list
    headers = {'authorization': token,                                                      #The header with the token and content type
               'content-type': 'application/json'}
    
    limit = 100                                                                             #limit of records per request (I choose 100 to make the less request possibles)
    after = 0                                                                               #Index of the next record since start the next request

    flagEndRecords = False                                                                  #To know if no are records remaining                                                                  
    recordsList = []                                                                         #To save the interes data on a list raw_email, country, phone ...

    print("Extracting records...")
    while(flagEndRecords == False):

        data = {'filterGroups': [                                                               #The body with filter for allowed_to_collect
                {
                    'filters': 
                    [
                        {
                            'value': 'true',
                            'propertyName': 'allowed_to_collect',
                            'operator': 'EQ'
                        }
                    ]
                }
            ],
            'properties': [
                'raw_email', 
                'country', 
                'phone', 
                'technical_test___create_date', 
                'industry', 
                'address', 
                'hs_object_id',
                'firstname',
                'lastname' 
            ], 'limit' : limit, 'after': after}
        
        response = requests.post(url, headers = headers, json = data)                           #Makes a post request to obtain the data in API
        records = response.json()                                                           #Returns the body response
        records = json.dumps(records)                                                   #Convert the data into a json string
        recordsDictionary = json.loads(records)                                               #Convert the json string into a dictionary
        
        totalRecords = recordsDictionary['total']                                           #To Know the total of records to extract

        recordsList = recordsList + [[i['properties']['raw_email'],                         #To convert record dictionary into a list
                 i['properties']['country'],
                 i['properties']['phone'],
                 i['properties']['technical_test___create_date'],
                 i['properties']['industry'],
                 i['properties']['address'],
                 i['properties']['hs_object_id'],
                 i['properties']['firstname'],
                 i['properties']['lastname']] for i in recordsDictionary['results']]

        if(after > totalRecords):                                                           #to check if you extract or not all the records and break the while
            flagEndRecords = True
        else:
            after = after + limit                                                           #the first element of the next page always jump according the limit value
    
    #print(recordsList)
    print(str(len(recordsList)) + " records has been extracted.")
    
    return recordsList                                                                      #return the records list that is the same of contacts extracted

def CountryRecognition(recordsList):                                                        #Function country recognition
    '''
    Description: Function to make the country recognition with the City saved on the recordsList (list of the extracted contacts),
    this function uses a JSON with the information of cities on each country. These are the steps in a summarized way that the 
    function follows:
        1. Load the JSON file 'Countries_and_cities.json'.
        2. Convert the JSON into a dictionary to manage the information easily.
        3. Enter into a for to iterate every contact.
        4. Takes the city of the contact (second index of the recordsList) and search it on the JSON file, according the result:
            4.1. If the city name is the name of a country, assigns the country has that value, and the city has None, for example, 
                 Ireland is a city on US but is a country too, in that case priorize the country name and puts (Ireland, None)
            4.2. If the city name is not a name of a country, takes the first coincidence and assigns the country and the city
                 for example (Canada, Oxford)

    Arguments:
        1. recordsList: List of the extracted contacts on funtion ContactCollection.
    
    Returns:
        1. recordsList: List with the contacts extracted, but with the country recognition function applied in the next format:
           (email, (country, city), phone, technical test created date, ...) 
    
    '''
    
    routeCountriesJson = 'Technical Phase 2_AmayaOscar/Countries_and_cities.json'           #The route of the json file with countries and cities data

    with open(routeCountriesJson) as contenido:                                             #load the json file
        datosJsonCountries = json.load(contenido)

    datosJsonCountries = json.dumps(datosJsonCountries)  
    datosJsonCountries = json.loads(datosJsonCountries)                                     #convert the json file into a dictionary

    country_found = ''                                                                      #name of the country
    city_found = ''                                                                         #name of the city
    city = ''                                                                               #name of the city in the records
    flagFound = False                                                                       #this flag finish the searching of country if finds the first

    print("Transforming countries...")
    for h in range(0,  len(recordsList)):                                                   #to iterate each city of every contact
        flagFound = False                                                                   #reload the flag
        city = recordsList[h][1]                                                            #the city of the contact
        
        for i in datosJsonCountries:                                                        #to iterate on the json file, it has a format of country:{city 1, city 2, city 3 ...}
            if(city is None):                                                               #if the city space in the record is none, return a none
                country_found = None
                city_found = None
                break

            if(city == i):                                                                  #priorize the value if can correspond to a country, for example exist a city called Ireland in US, but in this case going to take Ireland has country
                country_found = city                                                        
                city_found = None
                break


            for j in range(0, len(datosJsonCountries[i])):                                  #to iterate on every city of every country on json
                if(datosJsonCountries[i][j] == city):                                       #take the first coincidence and assign the values to been modified in the contact
                    country_found = i
                    city_found = datosJsonCountries[i][j]
                    flagFound = True
                    break

            if(flagFound == True):                                                          #breaks whit the first coincidence and proceed with the nexxt contact
                break

        recordsList[h][1] = (country_found, city_found)                                     #Modify the country space in the contact
    
    print(str(len(recordsList)) + " records has been country modified.")
    return(recordsList)                                                                     #returns the records of contacts modified

def CountryRecognitionAPI(recordsList):                                                     #Function country recognition by API
    '''
    Description: it makes a country recognition using a API, receive an returns the same that the last function, this method consume less
    desktop resources, but take a lot of time, I prefeer use the JSON method.
    '''
    
    url = 'https://countriesnow.space/api/v0.1/countries/population/cities'                 #API of countries and cities
    #data = {"city": "oxford"}

    # response = requests.post(url, json = data)
    # country = response.json()
    # country =  country['data']['country']
    # print(country)
    country_found = ''                                                                      #To save the country found in the API
    city_found = ''                                                                         #To save the city save in the API
    print("Transforming countries...")
    for i in range(0,  len(recordsList)):                                                   #To iterate in the ContactCollection list produced (records)

        data = {"city": recordsList[i][1]}                                                  #The position 1 of the list is the city, this is the json data for post request

        response = requests.post(url, json = data)                                          #Response of the API for the post request, here yo send the name of the city or country
        response = response.json()                                                          #parse the response to json format

        if(response['msg'] == 'city data not found'):                                       #if the city isn't exist in the API put the name o the city or the country in the country_found
            country_found = recordsList[i][1]
            city_found = 'not found'
        else:
            if(response['data']['country'] == 'United Kingdom of Great Britain and Northern Ireland'):      #change the name of United kingdom to easy the work to the fixed phone function
                country_found = 'United Kingdom'
                city_found = recordsList[i][1]
            else:
                country_found = response['data']['country']
                city_found = recordsList[i][1]

        recordsList[i][1] = (country_found, city_found)                                                     #assign the country to the city according the result of the API on the recordsList position 1(country position)
        #print(i)
    
    return recordsList                                                                                      #return the city and country for Country recognition

def FoundEmails(recordsList):                                                                               #function to emails
    '''
    Description: Function to return just an email on the email space, for example, parse from "Camilo <camilojimenez@correo.com> Jimenez"
    to "camilojimenez@correo.com", to make that:
        1. Iterate on every record using a for.
        2. Split the values after of ">" and before of "<".
        3. Exhange the corrected value (index 0 on recordsList).
    
    Arguments:
        1. recordsList: The list of extracted records with the country recognition aplied.
    
    Returns:
        1. recordsList: The list of extracted records with the emails fixed.
    '''
    
    foundEmail = ' '
    print("Transforming emails...")
    for i in range(0,  len(recordsList)):                                                                   #to iterate in the email column
        if(recordsList[i][0] is not None):                                                                  #detect a null in the email value
            foundEmail = recordsList[i][0]
            foundEmail = foundEmail.split('>', 1)[0]                                                        #only takes the values between <text>
            foundEmail = foundEmail.rsplit('<', 1)[1]
            recordsList[i][0] = foundEmail                                                                  #reasign the email space
    
    print(str(len(recordsList)) + " records has been email modified.")
    return recordsList                                                                                      #return the record list with the email correction

def FixPhoneNumbers(recordsList):                                                                            #function to fix number phones
    '''
    Desciption: Function to concatenate the dial code according the country of every contact, follows the next logic:
        1. Load a JSON ('Code_phones_countries.json') that contains the dial codes for every country.
        2. Takes the recordsList with the CountryRecognition function aplied.
        3. Search the country of every contact using a for, and search it on the JSON file.
        4. When finds the country, assigns the dial code concatenating the code + phone number and spliting all the "-" characters.

    Arguments:
        1. recordsList: The list of extracted contacts with the Country recognition and FoundEmails functions aplied.
    
    Returns:
        1. recordsList: The list of extracted contacts with the phone numbers fixed.
    '''
    
    routeJsonPhoneCodes = 'Technical Phase 2_AmayaOscar/Code_phones_countries.json'                                                      #the route of the JSON file

    with open(routeJsonPhoneCodes) as contenido:                                                            #to load the json file an save as a json variable
        datosJsonPhone = json.load(contenido)

    datosJsonPhone = json.dumps(datosJsonPhone)                                                               
    datosJsonPhone = json.loads(datosJsonPhone)                                                             #convert json into a dictionary
    
    countryName = ''                                                                                        #the value of country for each record
    print("Transforming phones...") 
    for i in range(0, len(recordsList)):                                                                    #to iterate on the records list
        countryName = recordsList[i][1][0]                                                                  #assign the countryName of the specific contact to searh it in the json
        for j in range(0, len(datosJsonPhone)):                                                             #to search in the phone codes json
            if (datosJsonPhone[j]['name'] == countryName and recordsList[i][2] is not None):                    #to asign the code to the number phone and delete the "-"
                recordsList[i][2] = '(' + datosJsonPhone[j]['dial_code'] + ')' + recordsList[i][2].replace('-', '')
                break

    print(str(len(recordsList)) + " records has been phone modified.")
    return(recordsList)                                                                                     #return the list with the emails fixed

def DuplicateManagement(recordsList):                                                                        #function to duplicate management
    '''
    Description: Function to merge the data existing in all the duplicate emails, uses pandas and follows the next logic:
        1. With a for, pull apart the country and city space, email, (country, city), phone ... -> email, country, city, phone ...
        2. Convert the list into a data frame
        3. Replace the Nones values to a NaN panda value.
        3. Using groupby (pandas function) according the emails, concatenate the Industry information separate with(;) and for the other 
           spaces takes the last updated value.
        4. Return the dataframe with the information of duplicates merged.

    Arguments:
        1. recordsList: List of the extracted contacts with the CountryRecognition, FoundEmails and FixedPhoneNumbers functions aplied.
    
    Returns:
        1. df: Dataframe with the duplicate management done.
    '''
    
    print('Merging duplicates...')
    for i in range(0, len(recordsList)):                                            
        recordsList[i] = recordsList[i][0], recordsList[i][1][0], recordsList[i][1][1], recordsList[i][2], recordsList[i][3], recordsList[i][4], recordsList[i][5], recordsList[i][6]           #Separate the country and city column

    df = pd.DataFrame(recordsList)                                                                           #convert the list into a dataframe
    df.columns = ['Email', 'Country', 'City', 'Phone Number', 'Original Create Date', 'Original Industry', 'Address', 'Temporary ID']           #name of columns in data Frame
    df['Original Create Date'] = pd.to_datetime(df['Original Create Date'])                                                                     #parse the string date into a date format
    df.sort_values(by = 'Original Create Date')                                                                                                 #sort the dates to take the last record modified
    df = df.fillna(value=np.nan)                                                                                                                #parse the None records to a panda NaN
    df = df.groupby('Email').agg({'Phone Number': 'first', 'Country': 'first', 'City': 'first', 'Address': 'last', 'Original Create Date': 'max', 'Original Industry' : 'first', 'Original Industry': ';'.join, 'Temporary ID': 'max'})     #merge the values according the email and taking the data from the last date record
    df['Original Industry'] = ';' + df['Original Industry']
    print(str(len(recordsList)-len(df)) + " records has been merged.")
    return df                                                                                                     #return the data frame

def SavingContacts(url, token, csvFile):                                                                     #function to upload the records with API
    '''
    Description: Function to load the contacts into the personal Hubspot account, the logic is the follow:
        1. On the main, we save a CSV file with the dataframe returned by the DuplicateManagement function, load and convert that CSV 
           into a list.
        2. Take groups of 50 elements to make a single post request.
        3. Builds the Body of the post request with the 50 contacts.
        4. Makes the request.
        5. prepare the next group of 50 elements, and repeat the process.
        6. saves the responses of the API.

    Arguments:
        1. url: URL of the batch create API.
        2. token: token of the personal account app.
        3. csvFile: the name of the csv file with the duplicatedManagement function aplied.
    
    Returns:
        --
    '''
   
    curl = url                                                                              #the URL of the batch create

    headers = {'authorization': token,                                                      #The header with the token and content type
               'content-type': 'application/json'}
    
    print("saving contacts on personal account...")
    
    with open(csvFile, 'r') as csv_file:                                                    #convert the CSV file to a list
        csv_reader = reader(csv_file)
        recordsList = list(csv_reader)
    
    del recordsList[0]                                                                      #delete the header of the CSV
    
    batchData =[]                                                                           #only is possible batch of 100, here we save the batch in groups
    responsesList = []                                                                      #a list of response for every batch
    properties = ''                                                                         #content of the batch
    after = 0                                                                               #to variate in record list
    limit = 50                                                                              #we going to give 50 jumps, it menas, batchs of 50 records
    index = 0                                                                               #the actual record getting saved on the properties
    
    while(index < len(recordsList)):                                                        #iterate to upload all the records
        for i in range(after, limit):                                                       #iterate in groups of 50 batchs
            properties = { "properties" : {                                                 #properties space on post request
                    "email" : recordsList[index][0],
                    "phone" : recordsList[index][1],
                    "country" : recordsList[index][2],
                    "city" : recordsList[index][3],
                    "original_create_date" : recordsList[index][5],
                    "original_industry" : recordsList[index][6],
                    "temporary_id" : recordsList[index][7]
                    }}
            batchData.append(properties)                                                    #all the records with all the properties in that batch
            index = index + 1                                                               #take the next record to save on properties
            if(index >=len(recordsList)):                                                   #if no exist more records, break
                break  

        data = {                                                                            #the json to send into the request
                'inputs': batchData
            }
        
        response = requests.post(curl, headers = headers, json = data)                      #makes the request
        responsesList.append(str(response))                                                 #saves the response on a list

        after = limit                                                                       #change the initial limit to the next batch
        limit = limit + 50                                                                  #change the finish limit to the next batch
        batchData = []                                                                      #clear to the next batch

    
    

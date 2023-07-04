import LibraryTechnicalPhaseOTF

if __name__ == '__main__':
    url = 'https://api.hubapi.com/crm/v3/objects/contacts/search'                   #URL of hotspot app
    token = 'Bearer pat-na1-3c7b0af9-bb66-40e7-a256-ce4c5eb27e81'                   #Token given

    records = LibraryTechnicalPhaseOTF.ContactCollection(url, token)                #Call to the collection function
    #print(records)

    records = LibraryTechnicalPhaseOTF.CountryRecognition(records)                  #Call to Country recognition function by JSON
    #records = LibraryTechnicalPhaseOTF.CountryRecognitionAPI(records)              #Call to Country recognition by API
    #print(records)
    
    records = LibraryTechnicalPhaseOTF.FoundEmails(records)                         #Call to found emails function
    #print(records)

    records = LibraryTechnicalPhaseOTF.FixPhoneNumbers(records)                     #Call to FixPhoneNumber function
    #print(records)

    records = LibraryTechnicalPhaseOTF.DuplicateManagement(records)                 #Call the duplicate management function
    print('Done!')
    print(records)

    csvName = 'ExtractionAndTransformation_AmayaOscar.csv'
    records.to_csv(csvName)

    url = 'https://api.hubapi.com/crm/v3/objects/contacts/batch/create'             #URL to upload the batch of records
    token = 'Bearer pat-na1-1e454217-6563-4179-bd0b-f0daf1c7c0b4'                   #The API Key on personal account

    #LibraryTechnicalPhaseOTF.SavingContacts(url, token, csvName)                    #Call the upload on personal Hubspot function
    
    



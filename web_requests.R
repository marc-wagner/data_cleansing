# web requests

library(RCurl)

test_webservice <- function() {

    headerFields =
      c(Accept = "text/xml",
        Accept = "multipart/*",
        'Content-Type' = "text/xml; charset=utf-8",
        SOAPAction = "https://advertising.criteo.com/API/v201010/clientLogin")
    
    body = '<?xml version="1.0" encoding="utf-8"?>
      <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Body>
      <clientLogin xmlns="https://advertising.criteo.com/API/v201010">
      <username>string</username>
      <password>string</password>
      <source>string</source>
      </clientLogin>
      </soap:Body>
      </soap:Envelope>'
    
    h = basicTextGatherer()
    
    curlPerform(url = "https://advertising.criteo.com/API/v201010/AdvertiserService.asmx",
                httpheader = headerFields,
                postfields = body,
                writefunction = h$update
    )
    
    h$value()
}

test_bpost_webservice_block <- function() {
  
  headerFields =
    c(Accept = "text/xml",
      Accept = "multipart/*",
      'Content-Type' = "text/xml; charset=utf-8",
      SOAPAction = "http://schema.bpost.be/services/common/address/ExternalMailingAddressProofingCS/v001/validateAddress")
  
  bodyMultiple = '<?xml version="1.0" encoding="utf-8"?>
      <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v001="http://schema.bpost.be/services/common/address/ExternalMailingAddressProofingCSMessages/v001">
   <soapenv:Header/>
   <soapenv:Body>
      <v001:ValidateAddressesRequest>
         <v001:AddressToValidateList>
            <!--1 to 200 repetitions:-->
            <v001:AddressToValidate id="1">
               <!--You have a CHOICE of the next 2 items at this level-->
               <!--Optional:-->
               <v001:AddressBlockLines>
                  <!--0 to 7 repetitions:-->
                  <v001:UnstructuredAddressLine locale="en">marc wagner 261 rue st laurent 4000 liege</v001:UnstructuredAddressLine>
               </v001:AddressBlockLines>
         </v001:AddressToValidate>
                     <v001:AddressToValidate id="2">
               <!--You have a CHOICE of the next 2 items at this level-->
               <!--Optional:-->
               <v001:AddressBlockLines>
                  <!--0 to 7 repetitions:-->
                  <v001:UnstructuredAddressLine locale="en">thomas dupont 259 rue st laurent 4000 liege</v001:UnstructuredAddressLine>
               </v001:AddressBlockLines>
         </v001:AddressToValidate>
            <v001:AddressToValidate id="3">
               <!--You have a CHOICE of the next 2 items at this level-->
               <!--Optional:-->
               <v001:AddressBlockLines>
                  <!--0 to 7 repetitions:-->
                  <v001:UnstructuredAddressLine locale="en">ben dover 10 rue st laurent 4000 liege</v001:UnstructuredAddressLine>
               </v001:AddressBlockLines>
         </v001:AddressToValidate>
         </v001:AddressToValidateList>
         <!--Optional:-->
         <v001:CallerIdentification>
            <v001:CallerName>klimaatzaak</v001:CallerName>
         </v001:CallerIdentification>
      </v001:ValidateAddressesRequest>
   </soapenv:Body>
</soapenv:Envelope>'

  bodySingle = '<?xml version="1.0" encoding="utf-8"?>
      <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v001="http://schema.bpost.be/services/common/address/ExternalMailingAddressProofingCSMessages/v001">
   <soapenv:Header/>
   <soapenv:Body>
      <v001:ValidateAddressesRequest>
         <v001:AddressToValidateList>
            <!--1 to 200 repetitions:-->
            <v001:AddressToValidate id="1">
               <!--You have a CHOICE of the next 2 items at this level-->
               <!--Optional:-->
               <v001:AddressBlockLines>
                  <!--0 to 7 repetitions:-->
                  <v001:UnstructuredAddressLine locale="en">marc wagner 261 rue st laurent 4000 liege</v001:UnstructuredAddressLine>
               </v001:AddressBlockLines>
         </v001:AddressToValidate>
         </v001:AddressToValidateList>
         <!--Optional:-->
         <v001:CallerIdentification>
            <v001:CallerName>klimaatzaak</v001:CallerName>
         </v001:CallerIdentification>
      </v001:ValidateAddressesRequest>
   </soapenv:Body>
</soapenv:Envelope>'
  
    
  h = basicTextGatherer() 
  
  curlPerform(url = "https://webservices-pub.bpost.be/ws/ExternalMailingAddressProofingCS_v1 HTTP/1.1",
              httpheader = headerFields
              ,postfields =   bodyMultiple
              ,writefunction = h$update
  )
  h$value()

}

test_bpost_webservice_itemized <- function() {
  
  headerFields =
    c(Accept = "text/xml",
      Accept = "multipart/*",
      'Content-Type' = "text/xml; charset=utf-8",
      SOAPAction = "http://schema.bpost.be/services/common/address/ExternalMailingAddressProofingCS/v001/validateAddress")
  
  bodySingle = '<?xml version="1.0" encoding="utf-8"?>
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v001="http://schema.bpost.be/services/common/address/ExternalMailingAddressProofingCSMessages/v001">
         <soapenv:Header/>
         <soapenv:Body>
            <v001:ValidateAddressesRequest>
               <v001:AddressToValidateList>
                  <v001:AddressToValidate id="1">
                     <v001:PostalAddress>
                        <v001:DeliveryPointLocation>
                           <v001:StructuredDeliveryPointLocation>
                              <v001:StreetName>SAINT LAUREN</v001:StreetName>
                              <v001:StreetNumber>261</v001:StreetNumber>
                              <v001:BoxNumber></v001:BoxNumber>
                           </v001:StructuredDeliveryPointLocation>
                        </v001:DeliveryPointLocation>
                        <v001:PostalCodeMunicipality>
                           <v001:StructuredPostalCodeMunicipality>
                              <v001:PostalCode>4000</v001:PostalCode>
                              <v001:MunicipalityName>LIGE</v001:MunicipalityName>
                           </v001:StructuredPostalCodeMunicipality>
                        </v001:PostalCodeMunicipality>
                        <v001:CountryName>BE</v001:CountryName>
                     </v001:PostalAddress>
                     <v001:DispatchingCountryISOCode>BE</v001:DispatchingCountryISOCode>
                     <v001:DeliveringCountryISOCode>BE</v001:DeliveringCountryISOCode>
                  </v001:AddressToValidate>
               </v001:AddressToValidateList>
               <v001:CallerIdentification>
                  <v001:CallerName>Klimaatzaak</v001:CallerName>
               </v001:CallerIdentification>
            </v001:ValidateAddressesRequest>
         </soapenv:Body>
      </soapenv:Envelope>'
  
  h = basicTextGatherer()
  
  curlPerform(url = "https://webservices-pub.bpost.be/ws/ExternalMailingAddressProofingCS_v1 HTTP/1.1",
              httpheader = headerFields
              ,postfields = bodySingle
              ,writefunction = h$update
  )
  h$value()
}

buildXlmAddress <- function(id, StreetName , StreetNumber, BoxNumber, PostalCode,MunicipalityName) {
  
  top2 = xmlTree("v001:AddressToValidate", namespaces= .(v001 = ""))
  top2$setNamespace("r")
  top2$addNode("v001:PostalAddress")
  top2$addNode("v001:DeliveryPointLocation" , close = TRUE)
  top2$addNode("v001:PostalCodeMunicipality", close = TRUE)
  top2$closeTag()  
  top2$value()
  
  top = newXMLNode("v001:AddressToValidate", attrs=c(id="1"))
  postalAddress = newXMLNode("v001:PostalAddress", parent=top)
  DeliveryPointLocation = newXMLNode("v001:DeliveryPointLocation", parent=postalAddress)
  StructuredDeliveryPointLocation = newXMLNode("v001:StructuredDeliveryPointLocation", parent=DeliveryPointLocation)
  newXMLNode("v001:StreetName", parent=StructuredDeliveryPointLocation)
  newXMLNode("v001:StreetNumber", parent=StructuredDeliveryPointLocation)
  newXMLNode("v001:BoxNumber", parent=StructuredDeliveryPointLocation)  
  PostalCodeMunicipality = newXMLNode("v001:PostalCodeMunicipality", parent=postalAddress)
  StructuredPostalCodeMunicipality = newXMLNode("v001:StructuredPostalCodeMunicipality", parent=PostalCodeMunicipality)
  newXMLNode("v001:PostalCode", parent=StructuredPostalCodeMunicipality)
  newXMLNode("v001:MunicipalityName", parent=StructuredPostalCodeMunicipality)  
  newXMLNode("v001:CountryName", parent=postalAddress)
  top
}

postSingleBpostValidationSoap <- function(id, StreetName , StreetNumber, BoxNumber, PostalCode,MunicipalityName){
 
  buildXlmAddress(id, StreetName , StreetNumber, BoxNumber, PostalCode,MunicipalityName) 
}

buildXml <- function(dt){
  
}

getjsonHeader <-function() {  
   '{"ValidateAddressesRequest": { "AddressToValidateList": { "AddressToValidate": ['
 # '{ValidateAddressesRequest: { AddressToValidateList: { AddressToValidate: ['
}

getjsonFooter <-function() {  
  # add 2 extra } at beginning to close the last item in enumeration due to replacement  of 
  # AddressBlockLines":"' by '"AddressBlockLines":{"UnstructuredAddressLine": {"*body
  '}}]   }, "CallerIdentification": {  "CallerName": "klimaatzaak" } } }'
 }

buildBpostValidateJsonBody <- function(dt){
   
  browser()
  jsonInput <- copy(dt)
  jsonInput[is.na(jsonInput)] <- ''  #replace NAs by blanks for building address
  jsonInput <- jsonInput[, paste(c(address,street_nb, address2, ',' ,zip, locality), collapse = ' ') ,by = id] 
  jsonInput <- jsonInput[, .("@id" = id, AddressBlockLines = V1)]
  
#JSON structure: either using fields or free text (see postman examples)
    # {
    #   "ValidateAddressesRequest": {
    #     "AddressToValidateList": {
    #       "AddressToValidate": [
    #         {
    #           "@id": "1",
    #           "PostalAddress": {
    #             "DeliveryPointLocation": {
    #               "StructuredDeliveryPointLocation": {
    #                 "StreetName": {
    #                   "@locale": "fr",
    #                   "*body": "Rue de la Loi"
    #                 },
    #                 "StreetNumber": "16",
    #                 "BoxNumber": "1"
    #               }
    #             },
    #             "PostalCodeMunicipality": {
    #               "StructuredPostalCodeMunicipality": {
    #                 "PostalCode": "1000",
    #                 "MunicipalityName": {
    #                   "@locale": "fr",
    #                   "*body": "Brussels"
    #                 }
    #               }
    #             }
    #           }
    #         },
    #         {
    #           "@id": "2",
    #           "AddressBlockLines": {
    #             "UnstructuredAddressLine": {
    #               "*body": "Rue de la Loi 14, 1000 Brussel"
    #             }
    #           }
    #         }
    #         ]
    #     },
    #     "CallerIdentification": {
    #       "CallerName": "klimaatzaak"
    #     }
    #   }
    # }
    
    
  
  # request_body <- data.frame(
  #   language = c("en","en"),
  #   id = c("1","2"),
  #   text = c("This is wasted! I'm angry","This is awesome! Good Job Team! appreciated")
  # )
  
  request_body_json <- toJSON(list(documents = as.data.frame(jsonInput[10:15,])), auto_unbox = TRUE) 
  #2 lines below are a hack because get cartesian product when building subtrees in data frame for nested JSON.
  request_body_json <- str_replace_all(request_body_json, coll('"AddressBlockLines":"')
                                                          ,'"AddressBlockLines":{"UnstructuredAddressLine": {"*body": "')
  request_body_json <- str_replace_all(request_body_json, coll('"},{"@id":'),'"}}},{"@id":')
  request_body_json <- str_replace(request_body_json, coll('{"documents":['), getjsonHeader())
  request_body_json <- str_replace(request_body_json, coll(']}')            , getjsonFooter())
  request_body_json
}




#https://stackoverflow.com/questions/39809117/how-to-post-api-in-r-having-header-json-body

postSingleBpostValidationRest <- function(){
}

postMultipleBpostValidationRest <- function(body){
  
  browser()
    
  result <- POST("https://webservices-pub.bpost.be/ws/ExternalMailingAddressProofingCSREST_v1/address/validateAddresses",
                 body = body,
                 add_headers(.headers = c("Content-Type"="application/json")))
  content(result)
}

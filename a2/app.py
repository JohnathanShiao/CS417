import json, requests
from flask import Flask, request
from geocodio import GeocodioClient

app=Flask(__name__)

GEO_URL = "https://api.geocod.io/v1.6/geocode"
YELP_URL = "https://api.yelp.com/v3/businesses/search"
GEO_API_KEY = "558151726d2518a584654c651acdd7326a14c17"
YELP_API_KEY = "TFAZR8uBGFCAdfRQsr2GIsrUjOY5t6XlWC3GLr0WOQL8Uu_DlH1AJiOEg0V1Nb9wOgPKElXOkd4SSZAZVdGsf1nzJIv04Hn40u0u8QfchJHhzoHH0Frx42OFoFl8YXYx"
HEADER = {
    "Authorization": "bearer {}".format(YELP_API_KEY)
    }
geo_client = GeocodioClient(GEO_API_KEY)
restaurant_list = list()

@app.route("/restaurant", methods=['GET','POST'])
def reply():
    if request.method == "GET":
        reply = {
            "restaurants": restaurant_list
        }
        return reply
    else:
        parameters = request.args
        body = request.get_json()
        #params and body could be empty
        if len(parameters)==0 and body is None:
            return "No info given", 400
        #params could be empty
        elif len(parameters)==0:
            if "address" not in body.keys():
                return "No address key", 400
            address = body.get("address")
        #body could be empty
        else:
            if "address" not in parameters.keys():
                return "No address key", 400
            address = parameters.get("address")
        if address == "" or address is None:
            return "No address given", 400
        try:
            coordinates = geo_client.geocode(address)
        except:
            return "Geocode invalid response", 500
        yelp_query = {
            "longitude": coordinates.get("results")[0].get("location").get("lng"),
            "latitude": coordinates.get("results")[0].get("location").get("lat"),
            "categories": "(food, ALL)"
        }
        data = requests.get(YELP_URL, params=yelp_query,headers=HEADER)
        if data.status_code != 200:
            return "Something went wrong with Yelp", 500
        yelp_reply = data.json()
        rest_list = yelp_reply["businesses"]
        for location in rest_list:
            temp = {
                "name": location.get("name"),
                "address": " ".join(location.get("location").get("display_address")),
                "rating": location.get("rating")
            }
            restaurant_list.append(temp)
        return "Success"


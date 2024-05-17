from collections import defaultdict
import pandas as pd
from algoliasearch.search_client import SearchClient
from datetime import datetime
from enum import Enum
import base64
import hashlib
import math

class TrailsDownloader:

    StateMap = {
        "Alabama": { "ID": 1, "Abbreviation": "AL", "BoundingBox": [-88.473227,30.223334,-84.88908,35.008028]},
        "Alaska": { "ID": 2, "Abbreviation": "AK", "BoundingBox": [-179.148909,51.214183,179.77847,71.365162]},
        "Arizona": { "ID": 3, "Abbreviation": "AZ", "BoundingBox": [-114.81651,31.332177,-109.045223,37.00426]},
        "Arkansas": { "ID": 4, "Abbreviation": "AR", "BoundingBox": [-94.617919,33.004106,-89.644395,36.4996]},
        "California": { "ID": 5, "Abbreviation": "CA", "BoundingBox": [-124.409591,32.534156,-114.131211,42.009518]},
        "Colorado": { "ID": 6, "Abbreviation": "CO", "BoundingBox": [-109.060253,36.992426,-102.041524,41.003444]},
        "Connecticut": { "ID": 7, "Abbreviation": "CT", "BoundingBox": [-73.727775,40.980144,-71.786994,42.050587]},
        "Delaware": { "ID": 8, "Abbreviation": "DE", "BoundingBox": [-75.788658,38.451013,-75.048939,39.839007]},
        "District_of_Columbia": { "ID": 9, "Abbreviation": "DC", "BoundingBox": [-77.119759,38.791645,-76.909395,38.99511]},
        "Florida": { "ID": 10, "Abbreviation": "FL", "BoundingBox": [-87.634938,24.523096,-80.031362,31.000888]},
        "Georgia": { "ID": 11, "Abbreviation": "GA", "BoundingBox": [-85.605165,30.357851,-80.839729,35.000659]},
        "Hawaii": { "ID": 6050, "Abbreviation": "HI", "BoundingBox": [-178.334698,18.910361,-154.806773,28.402123]},
        "Idaho": { "ID": 13, "Abbreviation": "ID", "BoundingBox": [-117.243027,41.988057,-111.043564,49.001146]},
        "Illinois": { "ID": 14, "Abbreviation": "IL", "BoundingBox": [-91.513079,36.970298,-87.494756,42.508481]},
        "Indiana": { "ID": 15, "Abbreviation": "IN", "BoundingBox": [-88.09776,37.771742,-84.784579,41.760592]},
        "Iowa": { "ID": 16, "Abbreviation": "IA", "BoundingBox": [-96.639704,40.375501,-90.140061,43.501196]},
        "Kansas": { "ID": 17, "Abbreviation": "KS", "BoundingBox": [-102.051744,36.993016,-94.588413,40.003162]},
        "Kentucky": { "ID": 18, "Abbreviation": "KY", "BoundingBox": [-89.571509,36.497129,-81.964971,39.147458]},
        "Louisiana": { "ID": 19, "Abbreviation": "LA", "BoundingBox": [-94.043147,28.928609,-88.817017,33.019457]},
        "Maine": { "ID": 20, "Abbreviation": "ME", "BoundingBox": [-71.083924,42.977764,-66.949895,47.459686]},
        "Maryland": { "ID": 21, "Abbreviation": "MD", "BoundingBox": [-79.487651,37.911717,-75.048939,39.723043]},
        "Massachusetts": { "ID": 22, "Abbreviation": "MA", "BoundingBox": [-73.508142,41.237964,-69.928393,42.886589]},
        "Michigan": { "ID": 23, "Abbreviation": "MI", "BoundingBox": [-90.418136,41.696118,-82.413474,48.2388]},
        "Minnesota": { "ID": 24, "Abbreviation": "MN", "BoundingBox": [-97.239209,43.499356,-89.491739,49.384358]},
        "Mississippi": { "ID": 25, "Abbreviation": "MS", "BoundingBox": [-91.655009,30.173943,-88.097888,34.996052]},
        "Missouri": { "ID": 26, "Abbreviation": "MO", "BoundingBox": [-95.774704,35.995683,-89.098843,40.61364]},
        "Montana": { "ID": 27, "Abbreviation": "MT", "BoundingBox": [-116.050003,44.358221,-104.039138,49.00139]},
        "Nebraska": { "ID": 28, "Abbreviation": "NE", "BoundingBox": [-104.053514,39.999998,-95.30829,43.001708]},
        "Nevada": { "ID": 29, "Abbreviation": "NV", "BoundingBox": [-120.005746,35.001857,-114.039648,42.002207]},
        "New_Hampshire": { "ID": 30, "Abbreviation": "NH", "BoundingBox": [-72.557247,42.69699,-70.610621,45.305476]},
        "New_Jersey": { "ID": 31, "Abbreviation": "NJ", "BoundingBox": [-75.559614,38.928519,-73.893979,41.357423]},
        "New_Mexico": { "ID": 32, "Abbreviation": "NM", "BoundingBox": [-109.050173,31.332301,-103.001964,37.000232]},
        "New_York": { "ID": 33, "Abbreviation": "NY", "BoundingBox": [-79.762152,40.496103,-71.856214,45.01585]},
        "North_Carolina": { "ID": 34, "Abbreviation": "NC", "BoundingBox": [-84.321869,33.842316,-75.460621,36.588117]},
        "North_Dakota": { "ID": 35, "Abbreviation": "ND", "BoundingBox": [-104.0489,45.935054,-96.554507,49.000574]},
        "Ohio": { "ID": 36, "Abbreviation": "OH", "BoundingBox": [-84.820159,38.403202,-80.518693,41.977523]},
        "Oklahoma": { "ID": 37, "Abbreviation": "OK", "BoundingBox": [-103.002565,33.615833,-94.430662,37.002206]},
        "Oregon": { "ID": 38, "Abbreviation": "OR", "BoundingBox": [-124.566244,41.991794,-116.463504,46.292035]},
        "Pennsylvania": { "ID": 39, "Abbreviation": "PA", "BoundingBox": [-80.519891,39.7198,-74.689516,42.26986]},
        "Rhode_Island": { "ID": 40, "Abbreviation": "RI", "BoundingBox": [-71.862772,41.146339,-71.12057,42.018798]},
        "South_Carolina": { "ID": 41, "Abbreviation": "SC", "BoundingBox": [-83.35391,32.0346,-78.54203,35.215402]},
        "South_Dakota": { "ID": 42, "Abbreviation": "SD", "BoundingBox": [-104.057698,42.479635,-96.436589,45.94545]},
        "Tennessee": { "ID": 43, "Abbreviation": "TN", "BoundingBox": [-90.310298,34.982972,-81.6469,36.678118]},
        "Texas": { "ID": 44, "Abbreviation": "TX", "BoundingBox": [-106.645646,25.837377,-93.508292,36.500704]},
        "Utah": { "ID": 45, "Abbreviation": "UT", "BoundingBox": [-114.052962,36.997968,-109.041058,42.001567]},
        "Vermont": { "ID": 46, "Abbreviation": "VT", "BoundingBox": [-73.43774,42.726853,-71.464555,45.016659]},
        "Virginia": { "ID": 47, "Abbreviation": "VA", "BoundingBox": [-83.675395,36.540738,-75.242266,39.466012]},
        "Washington": { "ID": 48, "Abbreviation": "WA", "BoundingBox": [-124.763068,45.543541,-116.915989,49.002494]},
        "West_Virginia": { "ID": 49, "Abbreviation": "WV", "BoundingBox": [-82.644739,37.201483,-77.719519,40.638801]},
        "Wisconsin": { "ID": 50, "Abbreviation": "WI", "BoundingBox": [-92.888114,42.491983,-86.805415,47.080621]},
        "Wyoming": { "ID": 51, "Abbreviation": "WY", "BoundingBox": [-111.056888,40.994746,-104.05216,45.005904]}
    }

    def __init__(self):
        self.client = SearchClient.create('9IOACG5NHE', '63a3cf94e0042b9c67abf0892fc1d223')
        self.index = self.client.init_index('alltrails_primary_en-US')

    def getHikesByState(self, state):

        hikes = dict()

        stateDetails = self.StateMap[state]
        lngMin, latMin, lngMax, latMax = stateDetails['BoundingBox']

        left = right = latMin
        step = 0.2

        queryParameters = {
            'filters': f"type:trail AND activities:hiking AND state_id={stateDetails['ID']} AND length>=0 AND elevation_gain>=0",
            'hitsPerPage': 1000
        }

        # AllTrails has restrictions on its API key so the only way to get
        # all hikes is to iterate over a bounding box surrounding the state.
        while right < latMax:
            right = min(latMax, right+step)
            queryParameters['insideBoundingBox'] = [[left, lngMin, right, lngMax]]
            response = self.index.search("", queryParameters)

            hits = response['hits']
            nbHits = response['nbHits']

            if nbHits < 1000:
                hikes.update({v['ID']:v for v in hits})
                left = right
                print(f"Processing {state}. Captured {len(hikes)} hikes", end='\r', flush=True)
            else:
                right = left
                step -= 0.05

        print(f"{state} Complete! Captured {len(hikes)} hikes" + " "*10)

        return hikes.values()


class TrailsExporter:
     
    class CsvHeader(Enum):
        Unique_Id = 0
        Alt_Id = 1
        Trail_Name = 2
        Source = 3
        Distance = 4
        Elevation_Gain = 5
        Highest_Point = 6
        Difficulty = 7
        Est_Hike_Duration = 8
        Trail_Type = 9
        Permits = 10
        Rating = 11
        Review_Count = 12
        Area = 13
        State = 14
        Country = 15
        Latitude = 16
        Longitude = 17
        Url = 18
        Cover_Photo = 19
        Parsed_Date = 20

    def getTrailAttribute(self, hike, attribute):

        truncate = lambda f, n: math.floor(f * 10 ** n) / 10 ** n

        match attribute:
            case self.CsvHeader.Unique_Id:
                uniqueName = self.getTrailAttribute(hike, self.CsvHeader.Url).partition('trail/')[2]
                source = self.getTrailAttribute(hike, self.CsvHeader.Source)
                latitude = self.getTrailAttribute(hike, self.CsvHeader.Latitude)
                longitude = self.getTrailAttribute(hike, self.CsvHeader.Longitude)

                joinKey = '&'.join([uniqueName, source, str(truncate(latitude, 3)), str(truncate(longitude, 3))])
                id = hashlib.sha1(joinKey.encode("UTF-8")).hexdigest()
                return id[:12]

            case self.CsvHeader.Alt_Id:
                return hike.get('ID', None)

            case self.CsvHeader.Trail_Name:
                return hike.get('name', None)

            case self.CsvHeader.Source:
                return "AllTrails"

            case self.CsvHeader.Distance:
                distanceMeters = hike.get('length', None)
                distanceMiles = round(distanceMeters*0.000621371192, 2) if distanceMeters else None
                return distanceMiles

            case self.CsvHeader.Elevation_Gain:
                elevationMeters = hike.get('elevation_gain', None)
                elevationFeet = int(elevationMeters / 0.3048) if elevationMeters else None
                return elevationFeet

            case self.CsvHeader.Highest_Point:
                elevationMeters = hike.get('highest_point', None)
                elevationFeet = int(elevationMeters / 0.3048) if elevationMeters else None
                return elevationFeet

            case self.CsvHeader.Difficulty:
                return hike.get('difficulty_rating', None)

            case self.CsvHeader.Est_Hike_Duration:
                return hike.get('duration_minutes_hiking', None)

            case self.CsvHeader.Permits:
                return None

            case self.CsvHeader.Trail_Type:
                trailMap = {'O': 'Out & Back', 'L': 'Loop', 'P': 'Point-to-Point'}
                type = hike.get('route_type', None)
                return trailMap.get(type, None) # Convert AllTrails route types to human readable

            case self.CsvHeader.Rating:
                return hike.get('avg_rating', None)

            case self.CsvHeader.Review_Count:
                return hike.get('num_reviews', None)
            
            case self.CsvHeader.Area:
                return hike.get('area_name', None)

            case self.CsvHeader.State:
                return hike.get('state_name', None)

            case self.CsvHeader.Country:
                return hike.get('country_name', None)

            case self.CsvHeader.Latitude:
                location = hike.get('_geoloc', None)
                return location.get('lat', None) if location else None

            case self.CsvHeader.Longitude:
                location = hike.get('_geoloc', None)
                return location.get('lng', None) if location else None

            case self.CsvHeader.Url:
                slug = hike.get('slug', None)
                return "https://www.alltrails.com/" + slug if slug else None

            case self.CsvHeader.Cover_Photo:
                photoLocation = hike.get('profile_photo_data', None)

                if photoLocation:
                    photoLocation = photoLocation.split('-')

                    assetData = ('{"bucket":"assets.alltrails.com",'
                                    f'"key":"uploads/photo/image/{photoLocation[0]}/{photoLocation[1]}.jpg",'
                                    '"edits":{"toFormat":"webp","resize":{"width":1080,"height":700,"fit":"cover"},'
                                    '"rotate":null,'
                                    '"jpeg":{"trellisQuantisation":true,"overshootDeringing":true,"optimiseScans":true,"quantisationTable":3}}}')

                    encodedData = base64.b64encode(bytes(assetData,'utf-8')) 
                    return "https://images.alltrails.com/" + encodedData.decode('utf-8')
                
                return None 

            case self.CsvHeader.Parsed_Date:
                return datetime.now().date().isoformat()

    def exportToCsv(self, hikes, exportPath):

        csvHeaders = defaultdict(list)

        for hike in hikes:
            for header in self.CsvHeader:
                csvHeaders[header.name].append(self.getTrailAttribute(hike, header))

        hike_data = pd.DataFrame(csvHeaders)
        hike_data.to_csv(exportPath, index = False)


if __name__ == "__main__":
    downloader = TrailsDownloader()
    exporter = TrailsExporter()

    hikesList = []
    for state in TrailsDownloader.StateMap.keys():
        hikes = downloader.getHikesByState(state)
        exporter.exportToCsv(hikes, f"./data/{state}_{datetime.now().date()}.csv")
        hikesList.extend(hikes)

    exporter.exportToCsv(hikesList, f"./data/AllTrails_Data_Full_{datetime.now().date()}.csv")
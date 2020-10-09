# For downloading the data set from the website

# url = 'https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page'


'''
# To mount the google drive to colab

from google.colab import drive
drive.mount('/content/drive')
'''

import requests
import urllib.request
import zipfile
import shapefile

# We are downloading the data of the first 5 months of 2019 - Jan, Feb, March, April & May

for month in range(1,6):
	file_url = "https://s3.amazonaws.com/nyc-tlc/trip+data/" + "yellow_tripdata_2019-{0:0=2d}.csv".format(month)

	# the data set is stored in the google drive
  	path = "/content/drive/My Drive/nyc-project/dataset/yellow_tripdata_2019-{0:0=2d}.csv".format(month)

  	r = requests.get(file_url, stream = True)
  	with open(path, "wb") as file:
    	for block in r.iter_content(chunk_size = 1024):
      		if block:
        		file.write(block)

  	print('{} done'.format(month))



# The pick-up and drop-off locations are encoded in LocationID
# To extract the LocationID data, we are downloading the taxis zones data
# The data is a zip file, extract the zip file to a 'shape' folder. 
# The folder will contains a 'taxi_zones.shp' that is used to extract the coordinates for the LocationID

urllib.request.urlretrieve("https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip", "taxi_zones.zip")

with zipfile.ZipFile("taxi_zones.zip","r") as zip_ref:
	zip_ref.extractall("./shape")

# function to extract the latitude and longitude coordinates from the 

def get_lat_lon(sf):
    content = []
    for sr in sf.shapeRecords():
        shape = sr.shape
        rec = sr.record
        loc_id = rec[shp_dic['LocationID']]
        
        x = (shape.bbox[0]+shape.bbox[2])/2
        y = (shape.bbox[1]+shape.bbox[3])/2
        
        content.append((loc_id, x, y))
    return pd.DataFrame(content, columns=["LocationID", "longitude", "latitude"])


sf = shapefile.Reader("shape/taxi_zones.shp")
fields_name = [field[0] for field in sf.fields[1:]]
shp_dic = dict(zip(fields_name, list(range(len(fields_name)))))
attributes = sf.records()
shp_attr = [dict(zip(fields_name, attr)) for attr in attributes]

df_loc = pd.DataFrame(shp_attr).join(get_lat_lon(sf).set_index("LocationID"), on="LocationID")

df_loc.to_csv('location_data.csv', index = False)
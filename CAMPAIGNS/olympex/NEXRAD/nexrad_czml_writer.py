import numpy as np
from copy import deepcopy
import json
from .helper.sample_data import sample_czml

czml_head = sample_czml[0]
model = sample_czml[1]

"""
The czml generation needs some variables where as some static properties:
varying things:
  - id: to identify each nexrad png image uniquely.
  - availability: time span for the visibility of the rectangle. rectangle used to show the image
         - availability is range. Determines when the rectangle is shown till when.
         - start point is easy to find. i.e. the start date time is embedded in the filename
         - but the end date time is not available.
         - will have to depend on the next filename, to determine the end date time.
  - coordinates: span for the rectangle to be shown.
         - is actually a constant for a single ground station.
  - image.uri: the url of the image to be shown in top of the rectangle.
"""

# class declaration

class NexradCzmlWriter:

    def __init__(self, location, image_height=0, background_rgba_color=[255, 255, 255, 128]):
        self.model = deepcopy(model)

        #some generic properties
        self.model["rectangle"]["coordinates"]["wsenDegrees"] = location
        self.model["rectangle"]["height"] = image_height
        self.model["rectangle"]["material"]["image"]["color"]["rgba"] = background_rgba_color

        self.czml_data = [czml_head]
        self.location = location
        self.image_height = image_height
        self.background_rgba_color = background_rgba_color
    
    def addTemporalImagery(self, id, imagery_url, start_date_time, end_date_time):
      new_node = deepcopy(self.model)
      new_node['id'] = id
      new_node['availability'] = f"{start_date_time}/{end_date_time}"
      new_node["rectangle"]["material"]["image"]["image"] = imagery_url
      self.czml_data.append(new_node)

    def get_string(self):
        return json.dumps(self.czml_data)

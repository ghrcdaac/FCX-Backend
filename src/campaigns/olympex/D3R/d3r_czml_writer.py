import numpy as np
from copy import deepcopy
import json
from sample_data import sample_czml

czml_head = sample_czml[0]
model = sample_czml[1]

# class declaration

class D3RCzmlWriter:
    def __init__(self):
      self.model = deepcopy(model)
      self.czml_data = [czml_head]
    
    def add3dTiles(self, id, tileset_url, start_date_time, end_date_time):
      """add 3d tileset url, that will be only available for a certain time.

      Args:
          id (number): unique identifier
          tileset_url (string): complete url for the 3dtile data (public s3 url)
          start_date_time (string): The time when the 3dtile is available/loaded/visible. Should be in the format "YYYY-MM-DDTHH:MM:00Z"
          end_date_time (string): The time when the 3dtile ceases to exist/is_removed. Should be in the format "YYYY-MM-DDTHH:MM:00Z"
      """
      new_node = deepcopy(self.model)
      new_node['id'] = f"d3r-3dtile-{id}"
      new_node['availability'] = f"{start_date_time}/{end_date_time}"
      new_node["tileset"]["uri"] = tileset_url
      self.czml_data.append(new_node)

    def get_string(self):
      """get the final czml

      Returns:
          string: czml data in string, that can be stored elsewhere.
      """
      return json.dumps(self.czml_data)

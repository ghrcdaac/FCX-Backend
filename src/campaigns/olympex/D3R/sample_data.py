
# A sample czml that will display the D3R time dynamic point cloud (3d tiles) data in cesium, for a single day.
sample_czml = [
    {
      "id": "document",
      "name": "CZML D3R",
      "version": "1.0",
      # "clock": {
      #   "interval": "2015-09-22T22:28:00Z/2015-09-22T23:58:00Z",
      #   "currentTime": "2015-09-22T22:28:00Z",
      #   "multiplier": 20,
      # },
    },
    {
      "id": "BatchedColors1",
      "name": "D3R",
      "availability": "2015-12-03T00:05:00Z/2015-12-03T00:20:08Z",
      "tileset": {
        "uri": 
          "https://ghrc-fcx-field-campaigns-szg.s3.amazonaws.com/Olympex/instrument-processed-data/D3R/20151203/freq-0/tileset.json",
      },
      # "point": {
      #   "pixelSize": 10
      # }
    },
  ]
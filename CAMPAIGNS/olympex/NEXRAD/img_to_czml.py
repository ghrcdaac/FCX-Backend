# A sample czml that will display the nexrad time dynamic imagery data in cesium
czml = [
    {
      "id": "document",
      "name": "CZML Geometries Rectangle",
      "version": "1.0",
      "clock": {
        "interval": "2015-09-22T22:28:00Z/2015-09-22T23:58:00Z",
        "currentTime": "2015-09-22T22:28:00Z",
        "multiplier": 20,
      },
    },
    {
      "id": "textureRectangle1",
      "name": "rectangle with image, above surface",
      "availability": "2015-09-22T22:29:00Z/2015-09-22T22:38:00Z",
      "rectangle": {
        "coordinates": {
          "wsenDegrees": [-123.197, 48.735, -121.812, 49.653],
        },
        "height": 0,
        "fill": True,
        "material": {
          "image": {
            "image": { "uri": "https://ghrc-fcx-field-campaigns-szg.s3.amazonaws.com/Olympex/instrument-raw-data/nexrad/katx/2015-09-22/olympex_Level2_KATX_20150922_2229_ELEV_01.png" },
            "color": {
              "rgba": [255, 255, 255, 128],
            },
          },
        },
      },
    },
    {
      "id": "textureRectangle2",
      "name": "rectangle with image, above surface",
      "availability": "2015-09-22T22:38:00Z/2015-09-22T22:48:00Z",
      "rectangle": {
        "coordinates": {
          "wsenDegrees": [-123.197, 48.735, -121.812, 49.653],
        },
        "height": 0,
        "fill": True,
        "material": {
          "image": {
            "image": { "uri": "https://ghrc-fcx-field-campaigns-szg.s3.amazonaws.com/Olympex/instrument-raw-data/nexrad/katx/2015-09-22/olympex_Level2_KATX_20150922_2238_ELEV_01.png", },
            "color": {
              "rgba": [255, 255, 255, 128],
            },
          },
        },
      },
    },
  ]


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
from gzip import open as gzip_open
import xarray as xr

def reader():
    fname = "/Users/Indhuja/Desktop/npol/impacts_NPOL1_2020_0225_000043_rhi.cf.gz"
    with gzip_open(fname, 'rb') as unzipped_file:
        with xr.open_dataset(unzipped_file) as data:
            print(data.variables)

reader()
import boto3
import os

def data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name):
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"{field_campaign}/{input_data_dir}/{instrument_name}/olympex"):
        keys.append(obj.key)

    result = keys
    for s3_raw_file_key in result:
        # SOURCE DIR.
        sdate = s3_raw_file_key.split('_')[3]
        print(f'processing CRS file {s3_raw_file_key}')

        # create a czml file.

        # UPLOAD CONVERTED FILES.
        output_czml = writer.get_string()
        output_name = os.path.splitext(os.path.basename(s3_raw_file_key))[0]
        output_name_wo_time = output_name.split("-")[0];
        outfile = f"{field_campaign}/{output_data_dir}/{instrument_name}/{output_name_wo_time}.czml"
        s3_client.put_object(Body=output_czml, Bucket=bucket_name, Key=outfile)
        print(s3_raw_file_key+" conversion done.")



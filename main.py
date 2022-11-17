from google.cloud import storage
import os.path
from zipfile import ZipFile
from zipfile import is_zipfile
import io
import logging
import google.cloud.logging
from datetime import datetime


logging_client = google.cloud.logging.Client()
logging_client.get_default_handler()
logging_client.setup_logging()

def zipextract(bucket_origin,bucket_destination, zipfilename):

    storage_client = storage.Client()
    current_date = datetime.today()

    bucket_origin = storage_client.get_bucket(bucket_origin)
    bucket_destination = storage_client.get_bucket(bucket_destination)
    destination_blob_pathname = '{0:04d}/{1:02d}/{2}'.format(current_date.year, current_date.month,"")
    blob = bucket_origin.blob(zipfilename)

    zipbytes = io.BytesIO(blob.download_as_string())

    if is_zipfile(zipbytes):
        with ZipFile(zipbytes, 'r') as myzip:
            for contentfilename in myzip.namelist():
                contentfile = myzip.read(contentfilename)
                #blob = bucket_origin.blob(destination_blob_pathname + "/" + contentfilename)
                blob = bucket_destination.blob(destination_blob_pathname+contentfilename)
                blob.upload_from_string(contentfile)

def main(request):
     ''' {
          "bucket_origin":"cs-zip_file",
          "bucket_destination":"cs-zip_file",
          "file_name":"test.py.zip"
          }
     '''
     x = request.get_json()
     logging.info('json recebido : {}'.format(x))

     bucket_origin = ''
     bucket_destination = ''
     file_name = ''
     
     try:
        if x and 'bucket_origin' and 'bucket_destination' and 'file_name' in x:
            bucket_origin = x["bucket_origin"]
            bucket_destination = x["bucket_destination"]
            file_name = x["file_name"]
            zipextract(bucket_origin,
            bucket_destination, 
            #'{0:04d}/{1:02d}/{2}'.format(current_date.year, current_date.month, file_name)
            file_name)
    
     except Exception as e:
        logging.exception(e)
        status = "ERROR"
        detail = str(e)
        return {"status": "error", "details": str(type(e).__name__)}, 400
    
     logging.info('status : success')
     return {"status": "success"}

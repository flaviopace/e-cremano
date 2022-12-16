import requests
from bs4 import BeautifulSoup as bs
from PyPDF2 import PdfReader
import re
import io
import os
import json
from google.cloud import vision, storage

SCAN_PAGE_MAX = 3

baseURL = 'http://www.e-cremano.it/cda/detail.jsp?otype=1027&id=102469&type=Permesso%20di%20Costruire&archive=true&page='
baseDownload = 'http://www.e-cremano.it/'


def parseAndUploadPdf(storeFile=False):
    pratiche = []
    for page in range(1, SCAN_PAGE_MAX):
        myurl = baseURL + str(page)
        print("parsing page {}: {}".format(page, myurl))

        req = requests.get(myurl)
        soup = bs(req.content, 'html.parser')

        for div in soup.findAll('div', {"class", "left"}):
            print(div.findAll('h1')[1])
        for link in soup.findAll('a', href=True):
            if "PAP" in link.text:
                fileIn = link['href'].split('/')[6]
                localname = '/Users/flaviopace/Downloads/' + fileIn
                pdflink = baseDownload + link['href']
                print(pdflink)
                # save locally
                pratiche.append(fileIn)
                if not isBlobsAvailable('pdf-ecremano', fileIn):
                    response = requests.get(pdflink)
                    with open(localname, 'wb') as f:
                        f.write(response.content)
                    uploadBlob('pdf-ecremano', localname, fileIn)

    return pratiche


def upload_to_bucket(blob_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""

    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(
        '/Users/unicondor/Downloads/pdf-ocr.json')

    print(buckets=list(storage_client.list_buckets()))

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)

    # returns a public url
    return blob.public_url


def convertPDF():
    # Google cloud
    client = vision.ImageAnnotatorClient()
    image = vision.Image()  # Py2+3 if hasattr(vision, 'Image') else vision.types.Image()
    image.source.image_uri = 'gs://pdf-ecremano/ALBOPRETORIO_FILE_435930.pdf'

    response = client.text_detection(image=image)
    print(response)

    for text in response.text_annotations:
        print('=' * 30)
        print(text.description)
        vertices = ['(%s,%s)' % (v.x, v.y) for v in text.bounding_poly.vertices]
        print('bounds:', ",".join(vertices))


def async_detect_document(gcs_source_uri, gcs_destination_uri, localfile):
    """OCR with PDF/TIFF as source files on GCS"""
    import json
    import re
    from google.cloud import vision
    from google.cloud import storage

    # Supported mime_types are: 'application/pdf' and 'image/tiff'
    mime_type = 'application/pdf'

    # How many pages should be grouped into each json output file.
    batch_size = 2

    client = vision.ImageAnnotatorClient()

    feature = vision.Feature(
        type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)

    gcs_source = vision.GcsSource(uri=gcs_source_uri)
    input_config = vision.InputConfig(
        gcs_source=gcs_source, mime_type=mime_type)

    gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
    output_config = vision.OutputConfig(
        gcs_destination=gcs_destination, batch_size=batch_size)

    async_request = vision.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config,
        output_config=output_config)

    operation = client.async_batch_annotate_files(
        requests=[async_request])

    print('Waiting for the operation to finish.')
    operation.result(timeout=420)

    # Once the request has completed and the output has been
    # written to GCS, we can list all the output files.
    storage_client = storage.Client()

    match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
    bucket_name = match.group(1)
    prefix = match.group(2)

    bucket = storage_client.get_bucket(bucket_name)

    # List objects with the given prefix, filtering out folders.
    blob_list = [blob for blob in list(bucket.list_blobs(
        prefix=prefix)) if not blob.name.endswith('/')]
    print('Output files:')
    for blob in blob_list:
        print(blob.name)

    # Process the first output file from GCS.
    # Since we specified batch_size=2, the first response contains
    # the first two pages of the input file.
    output = blob_list[0]

    json_string = output.download_as_string()
    response = json.loads(json_string)

    # The actual response for the first page of the input file.
    first_page_response = response['responses'][0]
    annotation = first_page_response['fullTextAnnotation']

    # Here we print the full text from the first page.
    # The response contains more information:
    # annotation/pages/blocks/paragraphs/words/symbols
    # including confidence scores and bounding boxes
    with open(localfile, 'wb') as f:
        f.write('Full text:\n'.encode())
        f.write(annotation['text'].encode())


def jsonToTxt(directory):
    # iterate over files in
    # that directory
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f) and "ALBO" in f:
            print(f)
            # Opening JSON file
            f = open(f)

            # returns JSON object as
            # a dictionary
            data = json.load(f)
            annotation = data['responses'][0]['fullTextAnnotation']
            text = annotation['text']
            # localfile = f.
            outfile = filename + '.txt'
            print(outfile)
            with open(outfile, 'wb') as out:
                out.write('Full text:\n'.encode())
                out.write(annotation['text'].encode())
            # match = re.match(r'sig(.*?), nat', text)
            # bucket_name = match.group(1)
            # prefix = match.group(2)

            # Iterating through the json
            # list
            # for i in data['text']:
            #     print(i)

            # Closing file
            f.close()


def getInfo(directory):
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):
            print(filename)
            f = open(f)
            text = f.read()
            match = re.match(r'(S|s)ig(.*)', text)
            if match:
                name = match.group(1)
                prefix = match.group(2)
            f.close()


def isBlobsAvailable(b_name, name):
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_or_name=b_name)

    storedPdf = []
    for bl in blobs:
        storedPdf.append(bl.name)

    if any(name in s for s in storedPdf):
        print("{} is already available".format(name))
        return True
    else:
        print("{} is new".format(name))
        return False


def uploadBlob(b_name, name, blob_name):
    client = storage.Client()
    bucket = client.get_bucket(b_name)
    blob = bucket.blob(blob_name)
    with open(name, "rb") as my_file:
        blob.upload_from_file(my_file)


if __name__ == "__main__":
    pr = parseAndUploadPdf()

    print('Checking json')
    for curpr in pr:
        if not isBlobsAvailable('pdf-ecremano', curpr.replace('.pdf','.txtoutput')):
            print("AI ML Processing PDF {}".format(curpr))
            googlepath = 'gs://pdf-ecremano/' + curpr
            async_detect_document(googlepath, googlepath + '.txt', curpr.replace('pdf', 'txt'))

    # convertPDF()
    # for currpr in pr:
    #     print("AI ML Processing PDF {}".format(currpr))
    #     googlepath = 'gs://pdf-ecremano/' + currpr
    #     async_detect_document(googlepath, googlepath + '.txt', currpr.replace('pdf', 'txt'))
    # jsonToTxt('/Users/flaviopace/Documents/repos/e-cremano/e-cremano')
    # getInfo('/Users/flaviopace/Documents/repos/e-cremano/e-cremano-txt')

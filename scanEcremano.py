import requests
from bs4 import BeautifulSoup as bs
from PyPDF2 import PdfReader
import re
import io
from google.cloud import vision, storage


baseURL = 'http://www.e-cremano.it/cda/detail.jsp?otype=1027&id=102469&type=Permesso%20di%20Costruire&archive=true&page='
baseDownload = 'http://www.e-cremano.it/'

def parseHTML():

    for page in range(1, 30):
        myurl = baseURL + str(page)
        print("parsing page {}: {}".format(page, myurl))

        req = requests.get(myurl)
        soup = bs(req.content, 'html.parser')

        for div in soup.findAll('div', {"class","left"}):
            print(div.findAll('h1')[1])
        for link in soup.findAll('a', href=True):
            if "PAP"  in link.text:
                localname = '/Users/unicondor/Downloads/' + link['href'].split('/')[6]
                pdflink = baseDownload + link['href']
                print(pdflink)
                #save locally
                response = requests.get(pdflink)
                with open(localname, 'wb') as f:
                    f.write(response.content)

                #ret = upload_to_bucket(link['href'].split('/')[6], localname, "pdf-ecremano")
                #print(ret)

                # try to read PDF, but they are images
                # response = requests.get(pdflink)
                # with io.BytesIO(response.content) as open_pdf_file:
                #     read_pdf = PdfReader(open_pdf_file)
                #     num_pages = read_pdf.getNumPages()
                #     #print(num_pages)
                #     PageObj = read_pdf.getPage(0)
                #     pagetxt = PageObj.extractText()
                #     m = re.search("Criscuolo", pagetxt)
                #     if m:
                #         print(m.group(0))


def upload_to_bucket(blob_name, path_to_file, bucket_name):
    """ Upload data to a bucket"""

    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(
        '/Users/unicondor/Downloads/pdf-ocr.json')

    print(buckets = list(storage_client.list_buckets()))

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

def async_detect_document(gcs_source_uri, gcs_destination_uri):
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
    print('Full text:\n')
    print(annotation['text'])

if __name__ == "__main__":
    #parseHTML()
    convertPDF()
    async_detect_document('gs://pdf-ecremano/ALBOPRETORIO_FILE_431231.pdf', 'gs://pdf-ecremano/ALBOPRETORIO_FILE_435930.txt')



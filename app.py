from __future__ import print_function
import json
import boto3
import io
import re
import csv


import logging
from flask import Flask

app = Flask(__name__)
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@app.route('/', methods=['GET'])
def lambda_handler(event=None, context=None):
    split_file()
    return 'hello from Flask!'


def split_file():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('testbulk-kk')
    bfile = s3.Object('testbulk-kk', 'file.csv')

    body = bfile.get()['Body']
    hasData = True
    leftover = ""

    y = 0
    toS3IO = io.BytesIO()
    while hasData:
        data = body.read(amt=500000)

        if data == '':
            hasData = False
            continue

        lines = None
        if leftover != "":
            lines = io.StringIO(leftover + data.decode())
        else:
            lines = io.StringIO(data.decode())

        reader = csv.reader(lines, lineterminator='\n',
                            quotechar='"', delimiter=",")
        lineItems = list(reader)

        leng = len(lineItems)

        if leng == 0:
            continue

        if leng > 1:

            outwriter = csv.writer(
                toS3IO, lineterminator='\n', quotechar='"', delimiter=",", quoting=csv.QUOTE_MINIMAL)
            outwriter.writerows(lineItems[:-1])
            y = y + 1

            bucket.put_object(Key='sub/part' + str(y) +
                              '.csv', Body=toS3IO.getvalue())
            toS3IO = io.BytesIO()

        stio = io.BytesIO()
        writer = csv.writer(stio, lineterminator='', quotechar='"',
                            delimiter=",", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(lineItems[-1:])
        leftover = stio.getvalue()

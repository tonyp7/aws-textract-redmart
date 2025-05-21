""" Demonstration of AWS Textract applied to Redmart PDF invoices """

import logging
from datetime import datetime
import tomllib
import glob
from pathlib import Path
from textractor import Textractor
from textractor.data.constants import TextractFeatures
from textractor.entities.table import Table
from textractor.entities.document import Document
from textractor.entities.lazy_document import LazyDocument
import numpy as np

def parse_redmart_date(value:str):
    """
    This function attempts to parse a string date into a datetime object,
    using a known Redmart date format

    :param value: the string representation of a date
    :return: a date object, or None if no date could be parsed

    The "redmart invoice formats" have been found in values such as:
        DELIVERY TIME : Friday, 22 June, 2018
        Invoice Date: : 23 June, 2018
        Issue date: 2019-10-31
    """

    formats = ['%d %B, %Y', '%A, %d %B, %Y', '%Y-%m-%d']

    for fmt in formats:
        try:
            d = datetime.strptime(value, fmt)
            if d is not None:
                return d
        except ValueError:
            continue

    #no date was found
    return None



def locate_invoice_date(document:Document | LazyDocument) -> datetime | None:
    """
    This function goes through the list of key/values of a Textractor document (or LazyDocument).
    If a key contains the word "date", it then attempts to parse the value as date object.

    :param document: an AWS Textractor document
    :return: a datetime object, or None if no date could be parsed

    It is worth noting that they are several dates in an invoice, such as invoice date, issue date,
    or delivery date. Here the first valid date we find is returned. This works because all these
    date should be in one week of one another and as such it's valid for the data extraction 
    needed.
    """

    for kv in document.key_values:
        if 'date' in kv.key.text.lower():
            t = kv.value.text.strip()
            d = parse_redmart_date(t)
            if d is not None:
                return d
    #no date was found
    return None



def locate_invoice_table(tables: list[Table]) -> int:
    """
    Within a typical list of tables found by Textract in a document, returns the index
    of the table that contains the invoice details list.

    :param tables: A list of textractor of table entities
    :return: index of the table, -1 if it count not be located
    """
    for i, t in enumerate(tables):
        df = t.to_pandas()
        row0 = df.values[0]

        #Product Name seems to always be in there
        if next((s for s in row0 if 'Product Name'.lower() in s.lower()), None) is not None:
            return i

    return -1


def process_invoice_file(input_file:str, extractor:Textractor,
                         s3_upload_path:str) -> Document | LazyDocument:
    """
    Process a local pdf invoice file, sending it to AWS Textract for analysis.

    :param file: the path to the invoice file to process
    :param s3_upload_path: a valid S3 bucket path that can be written by the AWS account
    :return: a Textractor Document object
    """
    document = extractor.start_document_analysis(
        input_file,
        s3_upload_path=s3_upload_path,
        features=[TextractFeatures.TABLES, TextractFeatures.FORMS],
        save_image=False
        )
    document.s3_polling_interval = 0.25
    return document


def export_textract_table_to_csv(t:Table, output_file:str, document_date:datetime=None):
    """
    Export an AWS Textractor table to CSV; adding a new column "Date" filled with the 
    document date.

    :param t: the Textractor document table to convert to CSV
    :param output_file: the path to the file that will be written
    :param document_date: the date to fill in the newly added date column

    Some very light processing of the table is done to make it ready for the next step.
    In particular, all the new lines are moved. AWS Textract tends to add new lines
    in cell values for no particular reason, which makes it difficult to have readable
    CSV.
    """
    #we force the header column to not be recognized as header this way
    #we can just remove the \r\n in the entire data including header column
    #downside is that we need to manually craft the first row
    #when adding the date to the dataframe
    df = t.to_pandas(use_columns=False)
    df = df.replace(r'\n',' ', regex=True)
    df = df.replace(r'\r',' ', regex=True)

    #date conversion to YYYY-MM-DD
    document_date_str = ''
    if document_date is not None:
        document_date_str = document_date.strftime('%Y-%m-%d')

    arrd = np.full(shape=len(df), fill_value=document_date_str)
    arrd = arrd.tolist()
    arrd[0] = 'Date'
    df['date'] = arrd

    df.to_csv(output_file, index=False, header=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Config file, contains the S3 upload bucket that is needed for AWS Textractor
    # Change this to adapt to your own AWS environment
    with open("config.toml", "rb") as f:
        config = tomllib.load(f)

    #AWS Textractor
    aws_extractor = Textractor(profile_name="default")

    #Process each pdf in the input folder, place CSV output in output folder
    #with the same name but as .csv
    for file in glob.glob(config['data']['input_folder'] + "/*.pdf"):
        out_file = config['data']['output_folder'] + '/' + Path(file).stem + '.csv'

        textractor_document = process_invoice_file(input_file=file, extractor=aws_extractor,
                                        s3_upload_path=config['aws']['s3_upload_path'])
        dt = locate_invoice_date(textractor_document)
        idx = locate_invoice_table(textractor_document.tables)
        if idx >= 0:
            export_textract_table_to_csv(textractor_document.tables[idx],
                                         output_file=out_file,
                                         document_date=dt)
            if dt is None:
                logging.warning( "File %s was processed without a date", file)
            else:
                logging.info( "File %s was processed successfully", file )

        else:
            logging.warning( "Script failed to locate an invoice detail list " \
                                "table in file %s", file )

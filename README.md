# AWS Textract Redmart

A real world use-case of AWS Textract, getting data out of PDF invoices from an online supermarket called "Redmart".

## Invoice

The invoices are from Redmart, an online supermarket. A masked sample pdf is provided under the data subfolder of this repository.
![image](https://tonypottier.com/content/images/2025/05/invoice-data-masked8b.png)

## Output CSV

The goal is to get an output CSV containing the invoice details, as well as an additional column containing the invoice date.
![image](https://tonypottier.com/content/images/2025/05/final-data-export-csv.png)

## Running the code

- Clone
- Install dependencies
- Make sure you have an AWS Account configured with under ~/.aws/credentials
- Create a S3 bucket and change s3_upload_path in config.toml

That's it. The code already provide a sample.pdf, but you can place your own invoices if you have any.

See also: https://tonypottier.com/extracting-form-data-from-pdfs-with-aws-textract/ for a walkthrough

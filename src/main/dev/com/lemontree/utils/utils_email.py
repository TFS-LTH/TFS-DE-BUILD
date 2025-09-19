import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

ses_client = boto3.client('ses', region_name='ap-south-1')

def send_email_with_attachments(notify_email_addresses, pdf_content, pdf_filename, zip_content, zip_filename, attached_message):
    msg = MIMEMultipart()
    msg['From'] = 'ltdt@lemontreehotels.com'
    msg['To'] = notify_email_addresses
    msg['Subject'] = f'SUBJECT'

    body = f"""
    <html>
    <body>
    <p>Hi,</p>
    <p>{attached_message}</p>
    <p>Thanks and Regards,</p>
    <p><b>Disclaimer : This is a system generated e-mail and it is not being monitored. Any reply that will be sent on this can't be acknowledged by anyone. Hence, please do not reply directly.</b></p>    
    </body>
    </html>
    """
    msg.attach(MIMEText(body, 'html'))

    if pdf_filename and pdf_content:
        # Attach PDF
        pdf_attachment = MIMEApplication(pdf_content, _subtype="pdf")
        pdf_attachment.add_header('Content-Disposition', 'attachment', filename=pdf_filename)
        msg.attach(pdf_attachment)

    if zip_content and zip_filename:
        # Attach ZIP
        zip_attachment = MIMEApplication(zip_content, _subtype="zip")
        zip_attachment.add_header('Content-Disposition', 'attachment', filename=zip_filename)
        msg.attach(zip_attachment)

    # Convert comma-separated string to list
    destinations_list = [email.strip() for email in notify_email_addresses.split(",")]

    try:
        print(f"Sending email to: {notify_email_addresses}")
        response = ses_client.send_raw_email(
            Source=msg['From'],
            Destinations=destinations_list,
            RawMessage={'Data': msg.as_string()}
        )
        print(f"Email sent successfully: {response}")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")

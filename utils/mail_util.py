from mailerpy import Mailer
from utils import logger
from config import SMTP_HOST, SMTP_PORT, MAIL_FROM, MAIL_PASSWORD

mailer = Mailer(mail_host=SMTP_HOST, mail_port=SMTP_PORT, mail_address=MAIL_FROM, mail_password=MAIL_PASSWORD)

def send_email(mail_to, mail_cc, mail_bcc, mail_subject, mail_body, mail_attachments):
    try:
        mailer.send_mail(
            to_address=mail_to.split(','), 
            mail_cc=mail_cc.split(',') if mail_cc is not None else None, 
            mail_bcc=mail_bcc.split(',') if mail_bcc is not None else None, 
            subject=mail_subject, 
            attachments=[mail_attachments] if mail_attachments is not None else None,
            mail_body=mail_body)
        return 1
    except Exception as e:
        logger.exception('Error sending mail', e)
        return 0
from utils import logger
from sqlalchemy.orm import Session
from config import kafka_topic
from utils import get_active_reports, send_report_to_queue, flush_producer, get_metastore_engine
from datetime import datetime, timedelta

yesterday_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
from_date_daily = str(yesterday_date) + ' 00:00:00' 
to_date_daily = str(yesterday_date) + ' 23:59:59'

## creating an extra variable for adding logic for monthly
from_date = from_date_daily
to_date = to_date_daily

def main():

    logger.info("Producing user records to topic {}. ^C to exit.".format(kafka_topic))
    try:
        engine = get_metastore_engine()
        with Session(engine) as session_metastore:
            reports_metadata = get_active_reports(session_metastore, schedule='DAILY', schedule_type='DAILY_AMS')
            if reports_metadata:
                for report in reports_metadata: 
                    report_key = str(report.report_id) + "-" + report.query_type
                    modified_report = {
                        "report_name": str(report.report_name),
                        "sql_query" : str(report.sql_query).replace("#from_date#", from_date).replace('#to_date#', to_date),
                        "db_connection": report.db_connection,
                        "create_zip_file": report.create_zip_file,
                        "mail_to": report.mail_to,
                        "mail_cc": report.mail_cc,
                        "mail_bcc": report.mail_bcc,
                        "mail_subject": report.mail_subject,
                        "mail_body": report.mail_body
                    }
                    send_report_to_queue(report_key, modified_report)
            else:
                logger.info('No reports to send in queue, going to sleep for now')
    except KeyboardInterrupt:
        flush_producer()
    except Exception as e:
       logger.exception("An error occurred: {}".format(str(e)))



if __name__ == "__main__":
    main()
    flush_producer()



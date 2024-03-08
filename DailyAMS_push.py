import json
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from config import db_connection
from utils import get_active_reports, send_report_to_queue, flush_producer, get_metastore_engine




if __name__ == '__main__':
    ## fetching active reports
    engine = get_metastore_engine()
    with Session(engine) as session_metastore:
        reports_metadata = get_active_reports(session_metastore, schedule='DAILY', schedule_type='DAILY_AMS')
        for report in reports_metadata:
            ## playing with individual report here

            # creating json type object for pushing to kafka
            report_key = str(report.report_id) + "_" + report.query_type
            report_value = {
                    "report_name": report.report_name,
                    "sql_query": report.sql_query,
                    "db_connection": report.db_connection,
                    "create_zip_file": report.create_zip_file,
                    "mail_to": report.mail_to,
                    "mail_cc": report.mail_cc,
                    "mail_bcc": report.mail_bcc,
                    "mail_subject": report.mail_subject,
                    "mail_body": report.mail_body
                }
            report_value = json.dumps(report_value)
            
            # pushing to kafka 
            send_report_to_queue(report_key, report_value)

            # now this is a json object, can be pushed to the kafka for queuing

    flush_producer()
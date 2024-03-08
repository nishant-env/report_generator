import json
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from config import db_connection
from utils import get_active_reports







if __name__ == '__main__':
    engine = create_engine(db_connection('DB_CONNECTION_METASTORE'), echo=True)
    ## fetching active reports
    with Session(engine) as session_metastore:
        reports_metadata = get_active_reports(session_metastore, schedule='DAILY', schedule_type='DAILY_AMS')
        for report in reports_metadata:
            ## playing with individual report here

            # creating json type object for pushing to kafka
            report_meta = {
                str(report.report_id) + report.query_type : {
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
            }
            report_meta = json.dumps(report_meta)
            

            # now this is a json object, can be pushed to the kafka for queuing

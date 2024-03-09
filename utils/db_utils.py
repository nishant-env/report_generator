import pandas as pd
import os,sys
from datetime import datetime
from sqlalchemy import select, create_engine
from models import Reports, MailProperties
from .log_utils import logger
from config import db_connection, CSV_PATH

def get_metastore_engine():
    engine = create_engine(db_connection('DB_CONNECTION_METASTORE'))
    return engine

def get_active_reports(session, schedule_type, schedule):
    try:
        fetch_query = select(
            Reports.id.label('report_id'),
            Reports.name.label('report_name'),
            Reports.sql_query,
            Reports.is_html,
            Reports.db_connection,
            Reports.query_type,
            Reports.create_zip_file,
            Reports.encryption_value,
            MailProperties.mail_to,
            MailProperties.mail_cc,
            MailProperties.mail_bcc,
            MailProperties.mail_subject,
            MailProperties.mail_body
        ).select_from(Reports).join(MailProperties, Reports.id == MailProperties.report_id).where(
            Reports.schedule==schedule, Reports.schedule_type==schedule_type, Reports.report_status=='ACTIVE'
        ).order_by(Reports.priority_level)

        logger.info(f'Fetching active report metadata for schedule: {schedule}, schedule_type: {schedule_type}')
        logger.debug(fetch_query)

        result = session.execute(fetch_query).all()

        logger.info("Successfully fetched reports")
        logger.debug(f'Fetched result: {result}')
        return result
    except Exception as e:
        logger.exception(e)



### sqlalchemy based approach for generating reports, this is quite memory intensive
def generate_report_file(report_name, sql_query, db_connection):

    engine = create_engine(db_connection('DB_CONNECTION_METASTORE'))
    with engine.begin() as session:
        result = session.execute(sql_query).all()
        columns = session.execute(sql_query).keys()
    
    result = pd.DataFrame(result, columns=columns)
    csv_path = os.path.abspath(CSV_PATH) + report_name.replace(' ', '_'.lower)+ str(datetime.today().date()) + '.csv'
    print(csv_path)
    result.to_csv(csv_path, index=False)
    return csv_path


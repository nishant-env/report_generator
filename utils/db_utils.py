import pandas as pd
import os,sys
from datetime import datetime
from sqlalchemy import select, create_engine, update, func
from models import Reports, MailProperties
from .log_utils import logger
from config import db_connection, CSV_PATH

def get_metastore_engine():
    engine = create_engine(db_connection('DB_CONNECTION_METASTORE'))
    return engine


def execute_sql_query(sql_query, db_datastore):
    try:
        
        engine = create_engine(db_connection(db_datastore))
        with engine.connect() as connection:
            result = connection.execute(sql_query)
            result = result.fetchall()
            return result
    except Exception as e:
        # Log the exception or handle it as needed
        logger.exception(f'Error executing SQL query: {e}')
        return None
   

    
            






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
        print("fetch_query:" ,fetch_query)
        result = session.execute(fetch_query).all()
        # result = session.execute(fetch_query)
        print("the result:",result)

        logger.info("Successfully fetched reports")
        logger.debug(f'Fetched result: {result}')
        return result
    except Exception as e:
        logger.exception(f'Error fetching active reports: {e}')
        return None


### sqlalchemy based approach for generating reports, this is quite memory intensive
def generate_report_file(report_name, sql_query, db_datastore, create_zip_file): 


    engine = create_engine(db_connection(db_datastore))
    with engine.begin() as session:
        result_proxy = session.execute(sql_query)  
        columns = result_proxy.keys() if result_proxy else []

        result = result_proxy.fetchall()
    # Check if result is not empty
    if result:   
        result_df = pd.DataFrame(result, columns=columns)
        try:
           
            if create_zip_file == True:
                csv_path = os.path.join(os.path.abspath(CSV_PATH), (report_name.replace(' ', '_').lower() + '_' + str(datetime.today().date()) + '.csv.gz'))
                result_df.to_csv(csv_path, index=False, compression='gzip')
                print("CSV Path (compressed):", csv_path)
            else:
                csv_path = os.path.join(os.path.abspath(CSV_PATH), (report_name.replace(' ', '_').lower() + '_' + str(datetime.today().date()) + '.csv'))
                result_df.to_csv(csv_path, index=False)
                print("CSV Path:", csv_path)

            logger.info(f'Generated CSV file for report {report_name}')
            return csv_path
        except Exception as e:
            logger.info("Error occurred:", e)
            return None
    else:
        logger.info("Query result is empty, skipping report file generation")
        return None
    

##### metastore updation functions
# updating last_scheduled
def update_last_scheduled(report_id):
    try:
        set_query = update(
                Reports
        ).where(Reports.id == report_id).values(last_scheduled=func.now())
        logger.debug(set_query)
        engine = get_metastore_engine()
        with engine.begin() as session:
            session.execute(set_query)
        logger.info(f'Updated last scheduled in metastore for report {report_id}')
    
    except Exception as e:
        logger.exception(f'Error updating last scheduled in metastore for report {report_id}', e)


## updating sent
def update_sent(report_id, report_name):
    try:
        set_query = update(
                Reports
        ).where(Reports.id == report_id).values(sent=1)
        logger.debug(set_query)
        engine = get_metastore_engine()
        with engine.begin() as session:
            session.execute(set_query)
        logger.info(f'Updated sent in metastore for report {report_name}')
    
    except Exception as e:
        logger.exception(f'Error updating sent in metastore for report {report_name}', e)

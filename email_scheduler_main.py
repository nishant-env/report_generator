from utils import logger
from sqlalchemy.orm import Session
from config import kafka_topic
from utils import get_active_reports, send_report_to_queue, flush_producer, get_metastore_engine, update_last_error
from datetime import datetime, timedelta
import nepali_datetime
import argparse

def main(schedule, schedule_type, from_date, to_date, yesterday_date, type):

    logger.info("Producing user records to topic {}. ^C to exit.".format(kafka_topic))
    try:
        engine = get_metastore_engine()
        with Session(engine) as session_metastore:
            reports_metadata = get_active_reports(session_metastore, schedule=schedule, schedule_type=schedule_type)
            if reports_metadata:
                for report in reports_metadata: 
                    report_key = str(report.report_id) + "-" + report.query_type
                    modified_report = {
                        "report_name": str(report.report_name),
                        "sql_query" : str(report.sql_query).replace("#from_date#", from_date).replace('#to_date#', to_date).replace('#current_date#', str(nepali_datetime.date.today().to_datetime_date()) + ' 00:00:00'),
                        "db_connection": report.db_connection,
                        "create_zip_file": report.create_zip_file,
                        "mail_to": report.mail_to,
                        "mail_cc": report.mail_cc,
                        "mail_bcc": report.mail_bcc,
                        "mail_subject": report.mail_subject,
                        "mail_body": report.mail_body,
                        "type": type,
                        "logical_date": str(yesterday_date)
                    }
                    send_report_to_queue(report_key, modified_report)
            else:
                logger.info('No reports to send in queue, going to sleep for now')
    except KeyboardInterrupt:
        flush_producer()
    except Exception as e:
       update_last_error(report_id=report_key.split('-')[0], error_message=f'{str(datetime.datetime.now())} - Error sending to queue')
       logger.exception("An error occurred: {}".format(str(e)))



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='eSewa Ams',
        description='This program schedules report for both daily and report, based on the argument passed'
    )
    parser.add_argument('schedule_type',default='daily', nargs='?', help='Type "daily" for daily report and "monthly" for monthly report', choices=['daily', 'monthly'])
    args = parser.parse_args()
    logger.info(f"Argument received, {args.schedule_type}")
    if args.schedule_type.lower() == 'monthly':
        ### run for monthly
        # check if today is the last day of month
        today_date = nepali_datetime.date.today().day
        current_year = nepali_datetime.date.today().year
        current_month = nepali_datetime.date.today().month
        days_in_current_month = nepali_datetime._days_in_month(current_year, current_month)
        if today_date == days_in_current_month:
            logger.info("Running for monthly report")
            yesterday_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
            from_date = str(nepali_datetime.date.today().to_datetime_date() - timedelta(days=days_in_current_month)) + ' 00:00:00'
            to_date = str(nepali_datetime.date.today().to_datetime_date() - timedelta(days=1)) + ' 23:59:59'
            main('MONTHLY', 'DEFAULT_AMS', from_date, to_date, yesterday_date, 'monthly')

        else:
            raise Exception('Not the last day of month')


    else:
        ### run for daily
        yesterday_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        from_date = str(yesterday_date) + ' 00:00:00' 
        to_date = str(yesterday_date) + ' 23:59:59'
        logger.info("Running for daily report")
        main('DAILY', 'DAILY_AMS', from_date, to_date, yesterday_date, 'daily')



    flush_producer()



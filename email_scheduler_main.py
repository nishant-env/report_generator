from utils import logger
from sqlalchemy.orm import Session
from config import kafka_topic, schema_registry_url
from utils import get_active_reports, get_metastore_engine, update_last_error
from utils.configuration_utils import producer_conf
from utils.kafka_utils.kafka_util_producer import ProducerUtility
from utils.serialization_utils.avro_utils import AvroUtil
from datetime import datetime, timedelta
import nepali_datetime
import argparse
from models import avro_schema_str, ReportModel

def main(schedule, schedule_type, from_date, to_date, yesterday_date, type):

    logger.info("Producing user records to topic {}. ^C to exit.".format(kafka_topic))
    try:
        engine = get_metastore_engine()
        avro_util = AvroUtil(schema_registry_url=schema_registry_url, avro_schema_str=avro_schema_str, kafka_topic=kafka_topic, serialization_model=ReportModel)
        producer_util = ProducerUtility(producer_conf=producer_conf, serializer=avro_util.avro_serialization_formatter, deserializer=avro_util.avro_deserialization_formatter)
        with Session(engine) as session_metastore:
            reports_metadata = get_active_reports(session_metastore, schedule=schedule, schedule_type=schedule_type)
            if reports_metadata:
                for report in reports_metadata: 
                    report_key = str(report.report_id) + "-" + str(yesterday_date.replace('-',''))
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
                    producer_util.send_report_to_queue(report_key, modified_report)
            else:
                logger.info('No reports to send in queue, going to sleep for now')
    except KeyboardInterrupt:
        producer_util.flush_producer()
    except Exception as e:
       update_last_error(report_id=report_key.split('-')[0], error_message=f'{str(datetime.datetime.now())} - Error sending to queue')
       logger.exception("An error occurred: {}".format(str(e)))



if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='eSewa Ams',
        description='This program schedules report for both daily and report, based on the argument passed'
    )
    parser.add_argument('schedule_type',default='daily', nargs='?', help='Type "daily" for daily report and "monthly_last" for monthly report at last of month and "monthly_first" for monthly report at first day of month', choices=['daily', 'monthly_last', 'monthly_first'])
    args = parser.parse_args()
    logger.info(f"Argument received, {args.schedule_type}")
    if args.schedule_type.lower() == 'monthly_last':
        ### run for monthly
        # check if today is the last day of month
        today_date = nepali_datetime.date.today().day
        current_year = nepali_datetime.date.today().year
        current_month = nepali_datetime.date.today().month
        days_in_current_month = nepali_datetime._days_in_month(current_year, current_month)
        month_name = str(current_year)+'_'+nepali_datetime.date(current_year, current_month, 1).strftime('%B')
        if today_date == days_in_current_month:
            logger.info("Running for monthly report")
            from_date = str(nepali_datetime.date.today().to_datetime_date() - timedelta(days=days_in_current_month)) + ' 00:00:00'
            to_date = str(nepali_datetime.date.today().to_datetime_date() - timedelta(days=1)) + ' 23:59:59'
            main('MONTHLY', 'DEFAULT_AMS', from_date, to_date, month_name, 'monthly')

        else:
            raise Exception('Not the last day of month')

    elif args.schedule_type.lower() == 'monthly_first':
        ### run for monthly
        # check if today is the first day of month
        today_date = nepali_datetime.date.today().day
        current_year = nepali_datetime.date.today().year
        previous_month = nepali_datetime.date.today().month - 1
        previous_month = 12 if previous_month == 0 else previous_month
        previous_year = current_year - 1 if previous_month == 12 else current_year
        days_in_previous_month = nepali_datetime._days_in_month(previous_year, previous_month)
        month_name = str(previous_year)+'_'+ nepali_datetime.date(previous_year, previous_month, 1).strftime('%B')
        if today_date == 1:
            logger.info("Running for monthly report")
            from_date = str(nepali_datetime.date.today().to_datetime_date() - timedelta(days=days_in_previous_month)) + ' 00:00:00'
            to_date = str(nepali_datetime.date.today().to_datetime_date() - timedelta(days=1)) + ' 23:59:59'
            main('MONTHLY', 'MONTH_FIRST_DAY', from_date, to_date, month_name, 'monthly')

        else:
            raise Exception('Not the First day of month')
    
    else:
        ### run for daily
        yesterday_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        from_date = str(yesterday_date) + ' 00:00:00' 
        to_date = str(yesterday_date) + ' 23:59:59'
        logger.info("Running for daily report")
        main('DAILY', 'DAILY_AMS', from_date, to_date, yesterday_date, 'daily')




from models import Reports, MailProperties
from sqlalchemy import select
from .log_utils import logger


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
            Reports.schedule==schedule, Reports.schedule_type==schedule_type
        ).order_by(Reports.priority_level)

        logger.info(f'Fetching active report metadata for schedule: {schedule}, schedule_type: {schedule_type}')
        logger.debug(fetch_query)

        result = session.execute(fetch_query).all()

        logger.info("Successfully fetched reports")
        logger.debug(f'Fetched result: {result}')
        return result
    except Exception as e:
        logger.exception(e)
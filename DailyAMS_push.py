from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from config import db_connection
from utils import get_active_reports







if __name__ == '__main__':
    engine = create_engine(db_connection('DB_CONNECTION_METASTORE'), echo=True)

    ## fetching active reports
    with Session(engine) as session_metastore:
        reports_metadata = get_active_reports(session_metastore, schedule='DAILY', schedule_type='DAILY_AMS')

import enum

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Enum, SMALLINT, ForeignKey
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.sql import func



class ReportStatus(enum.Enum):
    ACTIVE = 0
    INACTIVE = 1
    DELETED = 2


Base = declarative_base()

class Departments(Base):
    __tablename__ = 'departments'
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_date = Column(DateTime, server_default=func.NOW())
    last_modified_date = Column(DateTime, server_default=func.NOW())
    department_name = Column(String(255))
    

class Reports(Base):
    __tablename__ = 'reports'
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_date = Column(DateTime, server_default=func.NOW())
    last_modified_date = Column(DateTime, server_default=func.NOW())
    last_scheduled = Column(DateTime)
    generation_time_seconds = Column(Integer)
    name = Column(String(100))
    schedule = Column(String(50))
    report_status = Column(Enum(ReportStatus))
    sql_query = Column(LONGTEXT)
    last_error = Column(LONGTEXT)
    is_html = Column(Boolean)
    db_connection = Column(String(30))
    priority_level = Column(SMALLINT)
    query_type = Column(String(10))
    sent = Column(Boolean)
    schedule_type = Column(String(50))
    create_zip_file = Column(Boolean)
    encryption_value = Column(String(50))
    department_id = Column(Integer, ForeignKey(Departments.id))


class MailProperties(Base):
    __tablename__ = 'mail_properties'
    id = Column(Integer, primary_key=True, autoincrement=True)
    mail_to = Column(String(255))
    mail_cc = Column(String(255))
    mail_bcc = Column(String(255))
    mail_subject = Column(String(255))
    mail_body = Column(LONGTEXT)
    report_id = Column(Integer, ForeignKey(Reports.id), unique=True)


## the below script runs only if it is run as main program, so to create the table, run this program first
if __name__ == "__main__":
    import sys
    sys.path.append('..')
    from sqlalchemy import create_engine
    from config import db_connection


    Base.metadata.create_all(bind=create_engine(db_connection('DB_CONNECTION_METASTORE'), echo=True))

    
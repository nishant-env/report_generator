
class User(object):
    def __init__(self, report_name, sql_query, db_connection, create_zip_file,
                 mail_to, mail_cc, mail_bcc, mail_subject, mail_body):
        self.report_name = report_name
        self.sql_query = sql_query
        self.db_connection = db_connection
        self.create_zip_file = create_zip_file
        self.mail_to = mail_to
        self.mail_cc = mail_cc
        self.mail_bcc = mail_bcc
        self.mail_subject = mail_subject
        self.mail_body = mail_body



def user_to_dict(user, ctx):
    return {
        "report_name": user.report_name,
        "sql_query": user.sql_query,
        "db_connection": user.db_connection,
        "create_zip_file": user.create_zip_file,
        "mail_to": user.mail_to,
        "mail_cc": user.mail_cc,
        "mail_bcc": user.mail_bcc,
        "mail_subject": user.mail_subject,
        "mail_body": user.mail_body
    }



def get_schema():
    schema_str = """
        {
          "type": "record",
          "name": "User",
          "fields": [
            {"name": "report_name", "type": "string"},
            {"name": "sql_query", "type": "string"},
            {"name": "db_connection", "type": "string"},
            {"name": "create_zip_file", "type": "boolean"},
            {"name": "mail_to", "type": "string"},
            {"name": "mail_cc", "type": "string"},
            {"name": "mail_bcc", "type": "string"},
            {"name": "mail_subject", "type": "string"},
            {"name": "mail_body", "type": "string"}
          ]
        }
    """
    return schema_str






from json import dumps    
avro_schema_str = dumps(
        {
          "type": "record",
          "namespace": "com.eSewa",
          "name": "report",
          "fields": [
            {"name": "report_name", "type": "string"},
            {"name": "sql_query", "type": "string"},
            {"name": "db_connection", "type": "string"},
            {"name": "create_zip_file", "type": "boolean"},
            {"name": "mail_to", "type": "string"},
            {"name": "mail_cc", "type": ["null","string"]},
            {"name": "mail_bcc", "type": ["null","string"]},
            {"name": "mail_subject", "type": "string"},
            {"name": "mail_body", "type": "string"},
            {"name": "type", "type": ["null", "string"], "default": None},
            {"name": "logical_date", "type": ["null", "string"], "default": None}
          ]
        })


class ReportModel(object):
    """
    Any model defined should have the following format:
    1) A init method to initialize unique objects
    2) A get_dictionary_formatted class method (can be static, but I prefer class method here), that converts the object into dictionary
    3) A get_model_from_dict class method, that converts a dictionary into ReportModel instance
    """
    def __init__(self, report_name: str, sql_query: str, db_connection: str, create_zip_file: bool,
                 mail_to: str, mail_cc: str, mail_bcc: str, mail_subject: str, mail_body: str, type: str, logical_date: str):
        self.report_name = report_name
        self.sql_query = sql_query
        self.db_connection = db_connection
        self.create_zip_file = create_zip_file
        self.mail_to = mail_to
        self.mail_cc = mail_cc
        self.mail_bcc = mail_bcc
        self.mail_subject = mail_subject
        self.mail_body = mail_body
        self.type = type
        self.logical_date = logical_date
    
    @classmethod
    def get_dictionary_formatted(_, report_object, ctx) -> dict:
        return {
            "report_name": report_object.report_name,
            "sql_query": report_object.sql_query,
            "db_connection": report_object.db_connection,
            "create_zip_file": report_object.create_zip_file,
            "mail_to": report_object.mail_to,
            "mail_cc": report_object.mail_cc,
            "mail_bcc": report_object.mail_bcc,
            "mail_subject": report_object.mail_subject,
            "mail_body": report_object.mail_body,
            "type": report_object.type,
            "logical_date": report_object.logical_date
        } 
    
    @classmethod
    def get_model_from_dict(cls, report_dict, ctx):
        return cls(**report_dict)
    
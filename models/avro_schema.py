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
            {"name": "type", "type": ["null", "string"], "default": None}
          ]
        })
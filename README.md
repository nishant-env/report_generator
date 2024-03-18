# report_generator
This is an automated report generator which fetches report metadata such as queries, whom to send the report from the metastore, prepares the report based on the metadata.

The metadata will be pushed to kafka topic, from which queued reports will be fetched by a consumer which is the actual worker for the report generation and mailing.



The configuration file should be in the following format: (name it ".conf.ini") <br>
[DATABASES]<br>
DB_CONNECTION_METASTORE=*Keep the sqlalchemy connection string here for metastore* <br>
DB_CONNECTION_DATASTORE=*Keep the sqlalchemy connection string here for datastore* <br>


[MAIL_CONFIG] <br>
SMTP_HOST = <br>
SMTP_PORT = <br>
MAIL_FROM = <br>
MAIL_PASSWORD = *MAIL_APP_PASSWORD*
<br>

[PATHS]<br>
CSV_GENERATION_PATH=

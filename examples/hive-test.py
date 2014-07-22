import os
from apiarist.job import HiveJob

# temp dir ??
os.environ['S3_BASE_PATH'] = 's3://hivetests/scratch/'
os.environ['APIARIST_TMP_DIR'] = '/Users/max/Code/apiarist/temp/'
os.environ['AWS_ACCESS_KEY_ID'] = ''
os.environ['AWS_SECRET_ACCESS_KEY'] = ''


class EmailRecipientsSummary(HiveJob):

    def table(self):
        return 'emails_sent'

    def input_columns(self):
        return [
            ('day', 'STRING'),
            ('weekday', 'INT'),
            ('sent', 'BIGINT')
            ]

    def output_columns(self):
        return [
            ('year', 'INT'),
            ('weekday', 'INT'),
            ('sent', 'BIGINT')
            ]

    def query(self):
        q = "SELECT YEAR(day), weekday, SUM(sent)"
        q += "FROM emails_sent GROUP BY YEAR(day), weekday;"
        return q

if __name__ == "__main__":
    EmailRecipientsSummary().run('s3://hivetests/data/emails-sent-by-date.csv')

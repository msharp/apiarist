import sys
import os
#sys.path.append('Users/max/Code/apiarist/apiarist')

from apiarist.job import HiveJob

# temp dir ??
os.environ['S3_BASE_PATH']          = 's3://hivetests/scratch/'
os.environ['APIARIST_TMP_DIR']      = '/Users/max/Code/apiarist/temp/'
os.environ["CSV_SERDE_JAR_S3"]      = 's3://hivetests/jars/csv-serde-1.1.2-0.11.0-all.jar'
os.environ['AWS_ACCESS_KEY_ID']     = ''
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
        return "SELECT YEAR(day), weekday, SUM(sent) FROM emails_sent GROUP BY YEAR(day), weekday;"
 

if __name__ == "__main__":
    EmailRecipientsSummary().run('s3://hivetests/data/emails-sent-by-date.csv')

        

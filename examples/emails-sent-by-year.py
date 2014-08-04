import os
import sys
apiarist_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(apiarist_dir)
from apiarist.job import HiveJob

# temp dir ??
tmp_dir = os.path.join(apiarist_dir, 'temp')
os.environ['APIARIST_TMP_DIR'] = tmp_dir + '/'
os.environ['S3_BASE_PATH'] = 's3://hivetests/scratch/'
os.environ['AWS_ACCESS_KEY_ID'] = ''
os.environ['AWS_SECRET_ACCESS_KEY'] = ''


class EmailRecipientsSummaryByYear(HiveJob):

    def configure_options(self):
        super(EmailRecipientsSummaryByYear, self).configure_options()
        self.add_passthrough_option('--year', dest='year')

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
        q = "SELECT YEAR(day), weekday, SUM(sent) "
        q += "FROM emails_sent "
        q += "WHERE YEAR(day) = {0} ".format(self.options.year)
        q += "GROUP BY YEAR(day), weekday;"
        return q

# $ python emails-sent-by-year.py -r local --output-dir /some/temp/dir \
# >        emails-sent-by-date.csv
if __name__ == "__main__":
    EmailRecipientsSummaryByYear().run()

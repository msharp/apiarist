# Apiarist

A python 2.5+ package for defining Hive queries which can be run on AWS EMR.

It is, in its current form, only addressing a very narrow use-case. 
Reading large CSV files into a Hive database, running a Hive query, and outputting the results to a CSV file.

Future versions will endeavour to extend the input/output formats and be runnable locally.

It is modeled on [mrjob](https://github.com/Yelp/mrjob) and attempts to present a similar API and use similar common variables to cooperate with `boto`.

## A simple Hive job

You will need to provide four methods: 

  - `table()` the name of the table that your query will select from.
  - `input_columns()` the columns in the source data file.
  - `output_columns()` the columns that your query will output.
  - `query` the HiveQL query.

This code lives in `/examples`.

```python
from apiarist.job import HiveJob

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
    EmailRecipientsSummary().run()
```

## Try it out

Locally (must have a Hive server available):

    python email_recipients_summary.py -r local /path/to/your/local/file.csv

EMR:

    python email_recipients_summary.py -r emr s3://path/to/your/S3/files/

### Command-line options

Various options can be passed to control the running of the job. In particular the AWS/EMR options.

  - `-r` the run mode. Either `local` or `emr` (default is `local`)
  - `--output-dir` where the results of the job will go.
  - `--s3-scratch-uri` the bucket in which all the temporary files can go.
  - `--ec2-instance-type` the base instance type. Default is `m3.xlarge`
  - `--ec2-master-instance-type` if you want the master type to be different.
  - `--num-ec2-instances` number of instances (including the master). Default is `2`.
  - `--ami-version` the amir version. Default is `latest`.
  - `--hive-version`. Default is `latest`.

## License

Apiarist source code is released under Apache 2 License. Check LICENSE file for more information.

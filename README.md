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

EMR:

    python email_recipients_summary.py s3://path/to/your/file.csv

## License

Apiarist source code is released under Apache 2 License. Check LICENSE file for more information.

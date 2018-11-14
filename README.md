# Apiarist

A python 2.6+ package for defining Hive queries which can be run on AWS EMR.

It is, in its current form, only addressing a very narrow use-case.
Reading large text files into a Hive database, running a Hive query, and outputting the results to a text file.

File format can be CSV or similar - other delimiters can be specified.

The jobs are runnable locally, which is mainly for testing. You will need a local version of Hive which is in your `PATH` such that the command `hive -f /some/hive/script.hql` causes hive to execute the contents of the file.

It is heavily modeled on [mrjob](https://github.com/Yelp/mrjob) and attempts to present a similar API and use similar common variables to cooperate with `boto`.

## A simple Hive job

You will need to provide four methods:

  - `table` the name of the table that your query will select from.
  - `input_columns` the columns in the source data file.
  - `output_columns` the columns that your query will output.
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

### Try it out

Locally (must have a Hive server available):

    python email_recipients_summary.py -r local /path/to/your/local/file.csv

EMR:

    python email_recipients_summary.py -r emr s3://path/to/your/S3/files/

*NOTE: for the EMR command, you will need to supply some basic configuration.*

### Serde

Hive allows custom a serde to be used to define data formats in tables. Apiarist uses [csv-serde](https://github.com/ogrodnek/csv-serde) to handle the CSV format properly.

This serde also allows configuration of the delimiter, quoting character, and escape character. The defaults are, delimiter = `,`, quote character = `"`, escape character = `\`.

You can override the defaults in your job. You should be careful about escape sequences when doing so because the value needs to be written into a file.

It is best to define them as string literals. Example:

```python
from apiarist.job import HiveJob

class EmailRecipientsSummary(HiveJob):

    INFILE_DELIMITER_CHAR = r'\t'
    INFILE_QUOTE_CHAR = r"\'"
    INFILE_ESCAPE_CHAR = r'%'

    OUTFILE_DELIMITER_CHAR = r'\t'
    OUTFILE_QUOTE_CHAR = r'\"'
    OUTFILE_ESCAPE_CHAR = r"\\"
```

## Configuration

There are a range of options for providing job-specific configuration.

### Command-line options

Arguments can be passed to jobs on the command line, or programmatically with an array of options. Argument handling uses the [optparse](https://docs.python.org/2/library/optparse.html) module.

Various options can be passed to control the running of the job. In particular the AWS/EMR options.

  - `-r` the run mode. Either `local` or `emr` (default is `local`)
  - `--conf-path` use a YAML configuration file.
  - `--output-dir` where the results of the job will go.
  - `--label` Alternate label for the job. Default is job's class name.
  - `--owner` Who is running this job (if different from the current user). Default is `getpass.getuser()`, or `no_user` if that fails.
  - `--s3-scratch-uri` the bucket in which all the temporary files can go.
  - `--local-scratch-dir` this is where temporary file will be written.
  - `--s3-log-uri` write the logs to this location on S3.
  - `--ec2-instance-type` the base instance type. Default is `m3.xlarge`
  - `--ec2-master-instance-type` if you want the master type to be different.
  - `--num-ec2-instances` number of instances (including the master). Default is `2`.
  - `--ami-version` the ami version. Default is `latest`.
  - `--hive-version`. Default is `latest`.
  - `--iam-instance-profile` role for the EC2 instances on the cluster. Default is `EMR_EC2_DefaultRole`.
  - `--iam-service-role` role for the Amazon EMR service on the cluster. Default is `EMR_DefaultRole`.
  - `--s3-sync-wait-time` to configure how long to wait after uploading files to S3.
  - `--check-emr-status-every` configure the interval between each status check on a running job.
  - `--quiet` less logging
  - `--verbose` more logging
  - `--retain-hive-table` for local mode, keep the hive table to run further ad-hoc queries.
  - `--visible-to-all-users` make your cluster visible to all IAM users on the same AWS account. Set by default
  - `--no-visible-to-all-users` hide your cluster from other IAM users on the same AWS account


*NOTE: IAM roles will be mandatory for all users after June 30, 2015. These are set via the `--iam-instance-profile` and `--iam-service-role` options above.*

*See [Configure IAM Roles for Amazon EMR](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-iam-roles.html)*

### Configuration file

You can supply arguments to your job in a configuration file. It takes the same format as `mrjob` configuration.

The name of the arguments is different, using underscores instead of hyphens and omitting leading hyphens.
Config options are divided by the type of runner (local/emr) to allow provision of all options for a job in one file.

Below is a sample config file:

```yaml
runners:
  emr:
    aws_access_key_id: AABBCCDDEEFF11223344
    aws_secret_access_key: AABBCCDDEEFF1122334AABBCCDDEEFF
    ec2_instance_type: m3.xlarge
    num_ec2_instances: 5
    s3_scratch_uri: s3://myjobs/scratchspace/
    ami_version: 3.2.1
    hive_version: 0.13.1
  local:
    local_scratch_dir: /home/apiarist/temp/
```

Arguments supplied on command-line or in application code will override those supplied in the config file.

### Environment variables

Some environment variables are used when the value is not provided in other configuration methods.

`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for connecting to AWS.

`S3_SCRATCH_URI` a S3 base location where all the temporary file for the job will be written.

`APIARIST_TMP_DIR` where local files will be written during job runs. (This is overridden by the `--local-scratch-dir` option)

`CSV_SERDE_JAR_S3` a permanent location of the serde jar. If this is not set, Apiarist will automatically upload a copy of the jar to an S3 location in the scratch space.

### Passing options to your jobs

Jobs can be configured to accept arguments.

To do this, add the following method to your job class to configutr the options:

```python
def configure_options(self):
    super(EmailRecipientsSummary, self).configure_options()
    self.add_passthrough_option('--year', dest='year')
```

And then use the option by providing it in the command line arguments, like this:

    python email_recipients_summary.py -r local /path/to/your/local/file.csv --year 2014

Then incorporating it into your HiveQL query like this:

```python
def query(self):
    q = "SELECT YEAR(day), weekday, SUM(sent) "
    q += "FROM emails_sent "
    q += "WHERE YEAR(day) = {0} ".format(self.options.year)
    q += "GROUP BY YEAR(day), weekday;"
    return q
```

## Querying Hive locally

When developing a new query, you may want to fire up Hive to run it and test your syntax.

To generate the Hive table, run your job locally with the `--retain-hive-table` argument. After it terminates, run `hive` from the command line and you will get a Hive prompt.

Because `apiarist` uses a serde to interpret the text files for Hive, you will need to add this serde to the Hive session before your table can be read.

The command to do this will be something like:

    hive> ADD JAR /Users/max/.virtualenvs/apiarist/lib/python2.7/site-packages/apiarist/jars/csv-serde-1.1.2-0.11.0-all.jar;

Obviously, your path will be different, depending on where apiarist is installed.

Once this is done you can start running interactive HiveQL queries on your text data.

## License

Apiarist source code is released under Apache 2 License. Check LICENSE file for more information.

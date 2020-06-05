from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 file_format = "",
                 json_path = "",
                 *args, **kwargs):

        super(LoadToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.file_format = file_format
        self.json_path = json_path

    def execute(self, context):
        """
        redshift_conn_id: redshift cluster connection info.
        aws_credentials_id: necessary info needed to make AWS connection
        table: redshift cluster table we want to copy the data into.
        s3_bucket: source data in S3 bucket that has the files we want to copy from.
        s3_key: S3 key files of source data
        
        """
        self.log.info('LoadToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        
        if self.file_format == "JSON":
            copy_table = """
                COPY {table}
                FROM '{s3_path}'
                ACCESS_KEY_ID '{access_key}'
                SECRET_ACCESS_KEY '{secret_key}'
                {file_format} '{json_path}';
            """.format(table=self.table,
                       s3_path=self.s3_path,
                       access_key=credentials.access_key,
                       secret_key=credentials.secret_key,
                       file_format=self.file_format,
                       json_path=self.json_path)
         
        elif self.file_format == "CSV":
            copy_table = """
            COPY {table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{access_key}'
            SECRET_ACCESS_KEY '{secret_key}'
            {file_format};
            """.format(
                table=self.table,
                s3_path=self.s3_path,
                access_key=credentials.access_key,
                secret_key=credentials.secret_key,
                file_format=self.file_format)
        
        else:
            raise ValueError("File type should be JSON or CSV.")
        
        redshift.run(copy_table)
        
        self.log.info(f"Completed loading data from S3 to Redshift {self.table} table")
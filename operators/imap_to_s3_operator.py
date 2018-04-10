import boa
import csv
import json
import os
import shutil

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator

from imap_plugin.hooks.imap_hook import ImapHook


class ImapToS3Operator(BaseOperator):
    """
    S3 To Redshift Operator

    :param imap_conn_id:        The source imap connection id.
    :type imap_conn_id:         string
    :param imap_email:          Email to search for in imap.
    :type imap_email:           string
    :param imap_subject:        Subject to search for in imap.
    :type imap_subject:         string
    :param s3_conn_id:          The source s3 connection id.
    :type s3_conn_id:           string
    :param s3_bucket:           The source s3 bucket.
    :type s3_bucket:            string
    :param s3_key:              The source s3 key.
    :type s3_key:               string
    """

    template_fields = ('s3_key',)

    def __init__(self,
                 imap_conn_id,
                 imap_email,
                 imap_subject,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.imap_conn_id = imap_conn_id
        self.imap_email = imap_email
        self.imap_subject = imap_subject
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        imap_conn = ImapHook(self.imap_conn_id)
        s3_conn = S3Hook(self.s3_conn_id)
        tmp_dir = '/tmp/{key}'.format(key=self.s3_key)

        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)

        os.mkdir(tmp_dir)

        criteria = '(FROM "{imap_email}" SUBJECT "{imap_subject}" UNSEEN)'.format(imap_email=self.imap_email,
                                                                                  imap_subject=self.imap_subject)
        attachments = imap_conn.download_attachments(criteria, tmp_dir)

        file_name = '{tmp_dir}/{key}.jsonl'.format(tmp_dir=tmp_dir, key=self.s3_key)
        s3_upload_file = open(file_name, 'w')

        for attachment in attachments:
            with open(attachment, 'r', errors='replace') as f:
                reader = csv.reader(f)
                headers = [boa.constrict(header) for header in next(reader)]
                for row in reader:
                    json_line = {}
                    for index, col in enumerate(row):
                        json_line[headers[index]] = col
                    json.dump(json_line, s3_upload_file)
                    s3_upload_file.write('\n')

        s3_upload_file.close()

        s3_conn.load_file(file_name,
                          self.s3_key,
                          self.s3_bucket,
                          True)

        shutil.rmtree(tmp_dir)

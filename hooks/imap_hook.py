from datetime import datetime
import email
import imaplib
import boa

from airflow.hooks.base_hook import BaseHook


class ImapHook(BaseHook):
    def __init__(self, imap_conn_id='imap_conn_id'):
        self.imap_conn_id = imap_conn_id
        self.connection = self.get_connection(imap_conn_id)

        self.server = imaplib.IMAP4_SSL(self.connection.host)
        self.server.login(self.connection.login, self.connection.password)

    def download_attachments(self,
                             search_criteria,
                             output_dir,
                             mailbox='INBOX'):
        downloaded_files = []
        self.server.select(mailbox)
        search_result, emails = self.server.search(None, search_criteria)
        for mid in emails[0].split():
            fetch_result, item = self.server.fetch(mid, "(BODY.PEEK[])")
            email_body = item[0][1]
            message = email.message_from_bytes(email_body)
            date = datetime.strptime(message['date'][:-6],
                                     "%a, %d %b %Y %H:%M:%S %z").strftime('%Y_%m_%d_%H_%M_%S')

            if message.get_content_maintype() != 'multipart':
                self.server.store(mid, '+FLAGS', '\Seen')
                continue
            for part in message.walk():
                if (part.get_content_maintype() != 'multipart'
                   and part.get('Content-Disposition') is not None):
                    file_name = boa.constrict(part.get_filename()
                                                  .rsplit('.', 1)[0])
                    ext = part.get_filename().rsplit('.', 1)[1]
                    file_path = '{dir}/{date}_{filename}.{ext}'.format(dir=output_dir,
                                                                       date=date,
                                                                       filename=file_name,
                                                                       ext=ext)
                    open(file_path, 'wb').write(part.get_payload(decode=True))
                    downloaded_files.append(file_path)
                    self.server.store(mid, '+FLAGS', '\Seen')

        return downloaded_files

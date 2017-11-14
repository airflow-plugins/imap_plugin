from airflow.plugins_manager import AirflowPlugin
from imap_plugin.hooks.imap_hook import ImapHook
from imap_plugin.operators.imap_to_s3_operator import ImapToS3Operator


class ImapPlugin(AirflowPlugin):
    name = "ImapPlugin"
    hooks = [ImapHook]
    operators = [ImapToS3Operator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

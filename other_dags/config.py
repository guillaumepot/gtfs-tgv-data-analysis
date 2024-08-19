import os


"""
These vars act as schedulers
"""
dag_scheduler_scraping =  os.getenv('DAG_SCHEDULER_SCRAPING')
dag_scheduler_preprocessing =  os.getenv('DAG_SCHEDULER_PREPROCESSING')
dag_scheduler_train_evaluate = os.getenv('DAG_SCHEDULER_TRAIN_EVALUATE')
dag_scheduler_modelregistry = os.getenv('DAG_SCHEDULER_MODELREGISTRY')
dag_scheduler_model_predictions = os.getenv('DAG_SCHEDULER_MODEL_PREDICTIONS')


"""
Tasks Callbacks
"""

# alertOnFailure callback send an email alert in case of task failure

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


alert_on_failure = os.getenv('ALERT_ON_FAILURE')
alert_email_send = os.getenv('ALERT_EMAIL_SEND')
alert_email_receive = os.getenv('ALERT_EMAIL_RECEIVE')
mail_server = os.getenv('MAIL_SERVER')
mail_server_port = os.getenv('MAIL_SERVER_PORT')
mail_login = os.getenv('MAIL_LOGIN')
mail_password = os.getenv('MAIL_PASSWORD')



def alertOnFailure(context):
    """
    Sends an email alert in case of task failure.

    Args:
        context (dict): The context object containing information about the task instance.

    Returns:
        None
    """

    msg = MIMEMultipart()
    msg['From'] = alert_email_send
    msg['To'] = alert_email_receive
    msg['Subject'] = 'Airflow Task Failure Alert ' + str(context.get('task_instance').task_id)

    message = 'Airflow task failed: \n' + str(context)
    msg.attach(MIMEText(message))

    mailserver = smtplib.SMTP(mail_server, mail_server_port)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.login(mail_login, mail_password)
    mailserver.sendmail(alert_email_send, alert_email_receive, msg.as_string())
    mailserver.quit()
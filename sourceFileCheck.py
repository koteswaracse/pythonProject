import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
#from email.mime.base import MIMEBase
#from email import encoders

msg = MIMEMultipart()
msg['From'] = 'koteswaracse@gmail.com'
msg['To'] = 'koteswaracse@gmail.com'
msg['Subject'] = 'simple email in python'
message = 'here is the email'
msg.attach(MIMEText(message))

mailserver = smtplib.SMTP('smtp.gmail.com',587)
# identify ourselves to smtp gmail client
mailserver.ehlo()
# secure our email with tls encryption
mailserver.starttls()
# re-identify ourselves as an encrypted connection
mailserver.ehlo()
mailserver.login('koteswaracse@gmail.com', 'Bujji-@12')

mailserver.sendmail('koteswaracse@gmail.com','koteswaracse@gmail.com',msg.as_string())
mailserver.quit()
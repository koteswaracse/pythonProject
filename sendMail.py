import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

fromaddr = "koteswaracse@gmail.com"
toaddr = "kotids1234@gmail.com"

msg = MIMEMultipart()

msg['From'] = fromaddr
msg['To'] = toaddr
msg['Subject'] = "Test the Mail code for Python"

body = '''
Hi Team,

I am sending this mail for test my Python code which is written for send mail
'''

msg.attach(MIMEText(body, 'plain'))

filename = "test.txt"
attachment = open("C:\\test.txt", "rb")

part = MIMEBase('application', 'octet-stream')
part.set_payload((attachment).read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

msg.attach(part)

server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login(fromaddr, "Bujji-@12")
text = msg.as_string()
server.sendmail(fromaddr, toaddr, text)
server.quit()
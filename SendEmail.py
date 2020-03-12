from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

def sendEmail(message,date,type):

    # create message object instance
    msg = MIMEMultipart()
    

    # setup the parameters of the message
    password = "Jdv98701."
    msg['From'] = "info@rationalias.com"
    msg['To'] = "m474115@gmail.com"
    msg['Subject'] = "Rationalias " + type +  " Report " + str(date)
    
    # add in the message body
    msg.attach(MIMEText(message, 'plain'))
    
    #create server
    server = smtplib.SMTP('mail.rationalias.com: 587')
    server.starttls()
    
    # Login Credentials for sending the mail
    server.login(msg['From'], password)
    
    
    # send the message via the server.
    server.sendmail(msg['From'], msg['To'].split(","), msg.as_string())
    server.quit()

    print("successfully sent email to %s:" % (msg['To']))



"""
& d:/Python_Code/ActeLocale/Scripts/python.exe d:/Python_Code/ActeLocale/SG_funcs.py
"""

# using SendGrid's Python Library
# https://github.com/sendgrid/sendgrid-python
import ntpath
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *
import configs
import base64
from datetime import datetime
import ntpath


def size_of_files(list_paths):
    size_all=0
    for file in list_paths:
        size_all += os.path.getsize(file)
    # returns size in MB
    return round(size_all/2**20, 1)


def SG_attachments (pdf_paths_list):
    SG_attachments_list= []
    for pdf in pdf_paths_list:
        with open(pdf, 'rb') as f:
            data = f.read()

        encoded_file = base64.b64encode(data).decode()
        file_name = ntpath.basename(pdf)
        attachedFile = Attachment(
            FileContent(encoded_file),
            FileName(file_name),
            FileType('application/pdf'),
            Disposition('attachment')
        )

        SG_attachments_list.append(attachedFile)
    return SG_attachments_list
    


def Send_SG_with_attachment(TO_email, Template_id, pdf_paths, Unsub_id, groups_to_unsub,
        template_dict,
        FROM_email= "constantin@implicareplus.org", FROM_name = "Implicare Plus",
        SG_API_Key= configs.SGK1):
    #____________________________________________________________________________
    from_email = From(FROM_email, FROM_name)
    to_email = To(TO_email)
    message = Mail(
                from_email=from_email,
                to_emails= to_email
                )

    message.template_id = Template_id

    message.reply_to = ReplyTo(
            email= FROM_email,
            name= FROM_name
            )

    message.dynamic_template_data = template_dict

    #Attachment
    try:
        if (len(pdf_paths[0])>2) and (0.01 <= size_of_files(pdf_paths) <=7):
            message.attachment = SG_attachments(pdf_paths_list=pdf_paths)
    except IndexError: 
        pass #not attaching anything

    #______________________________________
    message.asm = Asm(
        group_id=GroupId(Unsub_id),
        groups_to_display=GroupsToDisplay(groups_to_unsub)
    )

    try:
        sg = SendGridAPIClient(SG_API_Key)
        response = sg.send(message)
        print("<<<SG Status Code>>> ",response.status_code)
        #print("<<<Body>>>\n",response.body)
        #print("<<<Headers>>>\n",response.headers)
    except Exception as e:
        print(e)

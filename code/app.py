import streamlit as st 
import requests
import boto3
import json
from scrape import scrape_transcripts
from load_css import local_css
from credentials import authorize_user
from signup import signup_user
import time
from boto3.dynamodb.conditions import Key, Attr
import pandas as pd
import io
import ast
from PIL import Image
from datetime import datetime


st.title('Named Entity Recognition for Earnings Transcript')
region = ''
client_sf = boto3.client('stepfunctions')
s3_client = boto3.client('s3', region_name = region)
TableName = ""
TableName_ = ""
dynamodb = boto3.resource('dynamodb', region_name=region)
table = dynamodb.Table(TableName)
table_ = dynamodb.Table(TableName_)
dynamodb_client = boto3.client('dynamodb', region_name = region)




local_css("style.css")

st.markdown("""
<style>
.sidebar .sidebar-content {
    color: white;
    background-color: #0A3648;
    background-image: linear-gradient(#20B2AA,#2F4F4F);
}
</style>
    """, unsafe_allow_html=True)

back = "<div style='height:400px;width:700px;overflow:auto;background-color:lavender;color:brown;scrollbar-base-color:gold;font-family:sans-serif;padding:10px;'>"

menu = ['Sign-In/Sign-Up', 'Entity Recognition (NER) Pipeline', 'Individual APIs', 'Pipeline Architecture']

st.sidebar.markdown('**Select Dashboard**')
choices = st.sidebar.selectbox("", menu)
st.sidebar.markdown("---")


if choices == 'Entity Recognition (NER) Pipeline':
    st.subheader('Serverless Entity Recognition (NER) Pipeline')


    st.write('NER Pipeline allows you to scrape any Earnings call Report from seeking alpha\
             and extract Named entities using AWS Comprehend. It allows you to use Masking, Anonymization\
             & De-identification to encrypt your output report which can be further used for Sentiment Analysis of earnings\
             report without revealing the PII')

    
    
    authorized = False
    st.markdown("---")
    user_input = ''
    st.markdown('Enter your **Username & Password for Authorized Access**')
    username = ''
    url = ''
    selected_entity = []
    selected_entity_ = []
    to_mask_ = []
    to_anonymize = []
    to_deidentify_wdr = []
    to_reidentify_ = []
    
    def_mask = ['DATE', 'QUANTITY']
    def_anon = ['PERSON', 'EVENT']
    def_deiden = ['TITLE'] 
    def_reiden = [ 'ORGANIZATION','LOCATION']

    links = []
    sfe = []
    username = st.text_input("Enter Username")
    token = st.text_input("Enter ID Token for Authentication", type='password')
    #authorize = st.button('Authorize')

    st.markdown("---")
    st.markdown('What do you want to do today?')
    operation = st.radio('', ('Run Entity Extraction Pipeline', 'Visualize Previously Extracted Entities'))
    if(operation == 'Run Entity Extraction Pipeline'):

        st.markdown('***Select Options for PIPELINE***')
        scrape = st.checkbox('Scrape Earnings Call Transcript', True)
        if scrape:
            st.markdown('**Select your Input type**')
            input_type = st.radio(' ', ('Text (Single URL link)', 'CSV File Upload (Multiple URL Links)'))
            if(input_type == 'Text (Single URL link)'):
                st.markdown('Enter **Seeking Alpha URL**')
                url = st.text_input(" ", '')
                links.append(url)
                st.write(url)
            if(input_type == 'CSV File Upload (Multiple URL Links)'):
                st.markdown('**Upload file with links in CSV format**')
                uploaded_file = st.file_uploader("Choose a file")
                if uploaded_file is not None:
                    #stringio = io.StringIO(uploaded_file.read())
                    dataframe = pd.read_csv(uploaded_file)
                    st.write(dataframe)
                    for val in dataframe['Links']:
                        st.write(val)
                        links.append(val)
        
        detect = st.checkbox('Extract Entities(NER) using AWS Comprehend', True)
        if detect:
            st.markdown('***Select Entity Labels to be Extracted***')
            entity_list = ['PERSON', 'ORGANIZATION','EVENT', 'TITLE', 'LOCATION', 'COMMERCIAL_ITEM', 'DATE', 'QUANTITY', 'OTHER']
            #defaultent = ['PERSON', 'ORGANIZATION','EVENT', 'TITLE', 'LOCATION', 'COMMERCIAL_ITEM', 'DATE', 'QUANTITY']
            selected_entity = st.multiselect("", entity_list, default=entity_list)

        deidentify = st.checkbox('Masking & De-Identification of Entities', True)
        if deidentify:
            st.markdown('***Select Entities to be Masked (####)***')
            to_mask_ = st.multiselect(" ", selected_entity, default=def_mask)
            st.markdown('***Select Entities to be Anonymized***')
            to_anonymize = st.multiselect("  ", selected_entity, default=def_anon)
            st.markdown('***Select Entities to be De-identified without Re-Identification***')
            to_deidentify_wdr = st.multiselect("   ", selected_entity, default=def_deiden)
            st.markdown('***Select Entities to be De-identified & Re-Identified***')
            to_reidentify_ = st.multiselect("    ", selected_entity, default=def_reiden)            
            
        reidentify = st.checkbox('Re-Identify Entities', True)

        authorized = ''
        auth_res = ''

        run = st.button('Run Pipeline')
    

        if run:
            try:
                auth_resp = table.get_item(
                    Key={
                        'CurrentUser': username,
                        'IdToken':token
                    })
                if auth_resp['Item']['AccessToken']:
                    auth_res = True
            except:
                auth_res = False
            if auth_res:
                if auth_resp['Item']['AccessToken']:
                    if datetime.strptime(auth_resp['Item']['TokenTime'], '%Y-%m-%d %H:%M:%S') > datetime.now():
                        authorized = True
                    else:
                        authorized = False
            else:
                authorized = False

            if authorized:
                st.success("Token Validated, Successly Signed In as {}".format(username))

                st.markdown("---")
                
                if len(links) > 0:
                    my_bar = st.progress(0)
                    for percent_complete in range(100):
                        time.sleep(0.001)
                        my_bar.progress(percent_complete + 1)
                    st.write('Initiating Pipeline...')
                    count = 0
                    #linkss = list(links)
                    for url in links:
                        try:
                            count += 1
                            st.markdown('Running Pipeline for link {}'.format(str(count)))
                            print('1')
                            scrape_file_name = scrape_transcripts(url)
                            scrape_file_path= 'scraper_output/{}'.format(scrape_file_name)
                            scrape_response = s3_client.get_object(Bucket='ner-recognized-entites', Key=scrape_file_path)
                            print('2')
                            original_text = scrape_response['Body'].read().decode('utf-8')
                            
                            st.markdown("---")
                            st.subheader('***Scraped Earnings Call Transcript from Seeking Alpha***')
                            st.markdown("---")
                            original_html = back + original_text + '</div>'

                            st.markdown(original_html, unsafe_allow_html=True)
                            timestr = time.strftime("%Y%m%d-%H%M%S")
                            Reidentify_opt = 'Permanent'
                            if reidentify:
                                Reidentify_opt = 'Reidentify'

                            sf_name = username + '_SFExecution' + timestr

                            sf_input = json.dumps({"body": {"file_name": scrape_file_name, 
                                           "ReversibleorPermanent":Reidentify_opt,
                                           "User": username, 
                                           "selected_entity": selected_entity,
                                           "mask_entities": to_mask_,
                                           "anonymize_entities":to_anonymize,
                                           "deidentify_entities": to_deidentify_wdr,
                                           "de_reidentify_entities": to_reidentify_
                                           }}, sort_keys=True)
                            response = client_sf.start_execution(
                            stateMachineArn='',
                            name=sf_name,
                            input= sf_input)

                            for _ in range(0, 40):
                                time.sleep(0.1)
                                execution_resp = client_sf.describe_execution(
                                        executionArn=response['executionArn']
                                        )
                                if execution_resp['status'] == 'SUCCEEDED':
                                    break
                            file_names = 'file_names.txt'
                            _file_path= 'filenames_output/{}'.format(file_names)
                            file_res = s3_client.get_object(Bucket='ner-recognized-entites', Key=_file_path)
                            file_res_ = file_res['Body'].read().decode('utf-8')
                            file_res_text = ast.literal_eval(file_res_)

                            entities_file_ = file_res_text['Body']['entity_file']
                            annonymize_file_ = file_res_text['Body']['annonymize_file']
                            reidentify_file = file_res_text['Body']['reidentify_file']
                            scraped_file = file_res_text['Body']['scraped_file']
                            add_to_db = dynamodb_client.put_item(
                            TableName = 'NERUserFileInfo',Item = {

                            'User' : {'S': str(username)},
                            'IdToken': {'S': str(token)},
                            'Entity' : {'S':entities_file_},
                            'Deidentify' : {'S':annonymize_file_},
                            'Reidentify' : {'S':reidentify_file},
                            'Scrape': {'S':scraped_file},
                            'Sfe_name' : {'S':sf_name}
                            })
                            
                            st.success('Pipeline completed for link {}'.format(str(count)))
                            
                        except:
                            st.markdown('Please provide a valid URL from Seeking Alpha')

        
                else:
                    st.write('Please provide a Input URL to scrape Transcript')
            else:
                st.success("Token could not be Validated or is expired")




    if(operation == 'Visualize Previously Extracted Entities'):

        st.markdown('Select from Your previous executions to Visualize Entities')
        try:
            item_resp = table_.scan(
            FilterExpression=Attr('User').eq(username)
            )
            for item in item_resp['Items']:
                sfe.append(item['Sfe_name'])
            #st.write(sfe)

        except:
                st.write('You have no Previous History')
            

        execution = st.selectbox('', sfe)

        st.markdown('***Select Options for Visualizing***')

        scrape = st.checkbox('Scraped Earnings Call Transcript', True)
        detect = st.checkbox('Extracted Entities(NER)', True)
        deidentify = st.checkbox('Masked, Anonymized & De-Identified Entities', True)
        reidentify = st.checkbox('Re-Identified Entities', True)

        st.sidebar.markdown('***Entity Labels***')
        entity_list = ['PERSON', 'ORGANIZATION','EVENT', 'TITLE', 'LOCATION', 'COMMERCIAL_ITEM', 'DATE', 'QUANTITY', 'OTHER']
        defaultent = ['PERSON', 'ORGANIZATION','EVENT', 'TITLE', 'LOCATION', 'COMMERCIAL_ITEM', 'DATE', 'QUANTITY']
        selected_entity_ = st.sidebar.multiselect("", entity_list, default=defaultent)
        authorized = ''
        auth_res = ''
        visualize = st.button('Visualize Entities')


        if visualize:
            try:
                auth_resp = table.get_item(
                    Key={
                        'CurrentUser': username,
                        'IdToken':token
                    })
                if auth_resp['Item']['AccessToken']:
                    auth_res = True
            except:
                auth_res = False
                
            if auth_res:
                if auth_resp['Item']['AccessToken']:
                    if datetime.strptime(auth_resp['Item']['TokenTime'], '%Y-%m-%d %H:%M:%S') > datetime.now():
                        authorized = True
                        
                    else:
                        authorized = False
            else:
                authorized = False

            if authorized:
                st.success("Token Validated, Successfully Signed In as {}".format(username))

                st.markdown("---")
            
                if execution:
                    
                # call dynamo db and fetch file names 
                    try:
                        file_resp = table_.scan(
                        FilterExpression=Attr('Sfe_name').eq(execution)
                        )
                        entities_file_ = file_resp['Items'][0]['Entity']
                        annonymize_file_ = file_resp['Items'][0]['Deidentify']
                        reidentify_file = file_resp['Items'][0]['Reidentify']
                        scrape_file = file_resp['Items'][0]['Scrape']

                    except:
                            st.write('Files could not be found')

                if scrape:

                    scrape_file_path= 'scraper_output/{}'.format(scrape_file)
                    scrape_response = s3_client.get_object(Bucket='ner-recognized-entites', Key=scrape_file_path)
                    
                    original_text = scrape_response['Body'].read().decode('utf-8')
                    
                    st.markdown("---")
                    st.subheader('***Scraped Earnings Call Transcript from Seeking Alpha***')
                    st.markdown("---")
                    original_html = back + original_text + '</div>'

                    st.markdown(original_html, unsafe_allow_html=True)


                if detect:
                    entity_file_path= 'entities_output/{}'.format(entities_file_)
                    entity_res = s3_client.get_object(Bucket='ner-recognized-entites', Key=entity_file_path)
                    entity_res_ = entity_res['Body'].read().decode('utf-8')
                    
                    entities_e = ast.literal_eval(entity_res_)['Entities']

                    entities = []
                    for entity in entities_e:
                        if entity['Type'] in selected_entity_:
                            entities.append(entity)

                    st.markdown("---")
                    st.subheader('***Extracted Named Entities (NER)***')
                    st.markdown("---")

                    entities_ = str(entities)
                    entity_html = back + entities_ + '</div>'

                    st.markdown(entity_html, unsafe_allow_html=True)

                    st.markdown("---")
                    st.subheader('***Entities Visualized within Earnings Transcript***')
                    st.markdown("---")

                    color_lookup = {'PERSON':'tomato', 'ORGANIZATION':'aqua','EVENT':'dred', 'TITLE':'orchid', 'LOCATION':'blue', 'COMMERCIAL_ITEM':'red', 'DATE':'coral', 'QUANTITY':'pink', 'OTHER':'greenyellow'}
                    start = 0
                    orignial_html = '<div>'
                    for entity in entities:
                        orignial_html += original_text[start:entity['BeginOffset']] + "<span class='highlight " +  color_lookup[entity['Type']] + "'>" + entity['Text'] + "<span class='bold'>" + entity['Type'] + "</span></span>"

                        start = entity['EndOffset']
                    orignial_html += '</div>'
                    st.markdown(orignial_html, unsafe_allow_html=True)

                if deidentify:
                    st.markdown("---")
                    st.subheader('***Masked, Anonymized & De-Identified Transcript***')
                    st.markdown("---")
                    
                    mask_file_path= 'annonymize_output/{}'.format(annonymize_file_)
                    mask_res_ = s3_client.get_object(Bucket='ner-recognized-entites', Key=mask_file_path)
                    deidentified_message = mask_res_['Body'].read().decode('utf-8')
                    deidentify_html = back + deidentified_message + '</div>'

                    st.markdown(deidentify_html, unsafe_allow_html=True)

                if reidentify:
                    st.markdown("---")
                    st.subheader('***Re-Identification of De-Identified Entities***')
                    st.markdown("---")

                    reid_file_path= 'reidentify_output/{}'.format(reidentify_file)
                    reid_res = s3_client.get_object(Bucket='ner-recognized-entites', Key=reid_file_path)
                    re_identified_message = reid_res['Body'].read().decode('utf-8')
                    
                    reidentify_html = back + re_identified_message + '</div>'

                    st.markdown(reidentify_html, unsafe_allow_html=True)
                    st.markdown("---")



elif choices == 'Pipeline Architecture':
    image = 'AWS_architecture.png'
    arch_image = Image.open(image)
    st.image(arch_image, use_column_width=True)


elif choices == 'Individual APIs':
    st.write('')


elif choices == 'Sign-In/Sign-Up':
    st.sidebar.markdown('**Login/SignUp to use our Services**')

    radio = st.radio('', ('Login - Existing User', 'SignUp - New User'))
    st.markdown('---')
    if(radio == 'Login - Existing User'):
        st.markdown('**Login to get Your Authorized Access Token**')
        st.markdown('**Enter UserName**')
        username = st.text_input("")
        st.markdown('**Enter Password**')
        password = st.text_input(" ", type='password')
        login = st.button('Login')

        if login:
            authorized, username, IdToken = authorize_user(username, password)
            if authorized:
                st.success("Signed In as {}".format(username))
                st.markdown('**Your ID Token**')
                st.write(IdToken)
            else:
                st.success("User {} could not be Authorized".format(username))

    if(radio == 'SignUp - New User'):
        st.markdown('**SignUp to get Your Authorized Access Token**')
        st.markdown('**Enter UserName**')
        username = st.text_input("")
        st.markdown('**Enter Password**')
        password = st.text_input(" ", type='password')
        
        signup = st.button('SignUp')
        sign = ''
        message = ''

        if signup:
            message, sign = signup_user(username, password)
            if sign:
                st.success(message)
                
            else:
                st.success(message)



    #else:
    #    pass



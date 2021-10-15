import pandas as pd
import numpy as np
import psycopg2
import time
#from st_aggrid import AgGrid

import streamlit as st


@st.cache(allow_output_mutation=True, hash_funcs={"_thread.RLock": lambda _: None})
def init_connection():
    return psycopg2.connect(**st.secrets["postgres_prod"])

conn2 = init_connection()

@st.cache(ttl=600)
def run_query(query):
    with conn2.cursor() as cur:
        cur.execute(query)
        conn2.commit()
st.header('vcubo EVENTS UPLOAD APP')
st.caption('beta v0.6')

st.subheader('1. EVENT DATA INPUT')
event_types = ['-', 'SOCIAL', 'MANAGEMENT', 'ENGINEERING', 'PROCUREMENT', 'CONSTRUCTION', 'COMM/OPPERATIONS', 'QUALITY', 'HR', 'HSE']
with st.expander('', expanded=True):
    a01, a02 = st.columns(2)
    with a01:
        company_id = st.text_input("COMPANY ID:")
        st.session_state.projects_df = pd.read_sql(f"SELECT * FROM pr_main WHERE com_id = '{company_id}'", conn2)
        pr_list_company = ['-']+st.session_state.projects_df.l1_id.unique().tolist()
        project_sel = st.selectbox("SELECT PROJECT",pr_list_company )
    with a02:
        company_pass = st.text_input("COMPANY PASSWORD:")
        st.markdown('PROJECT DESCRIPTION')
        st.write('Project description spaceholder here - see how many characteres')

    projects_view = 'l1_id'
    #st.session_state.events_df = pd.read_sql(f'SELECT {projects_view} FROM events_main', conn2)
    st.session_state.project_act_summ = pd.read_sql(f"SELECT phase, bl_start, bl_finish, ac_start, ac_finish, l2_id  FROM pr_main WHERE l1_id = '{project_sel}'", conn2)
    columns_desc = {"phase":"PHASE", "bl_start":"BL START", "bl_finish":"BL FINISH", "ac_start":"ACTUAL START", "ac_finish":"ACTUAL FINISH", "l2_id":"TASK ID"}
    st.session_state.project_act_summ = st.session_state.project_act_summ.rename(columns=columns_desc, inplace=False)
    index_sel = ['-']+[(str(i)+f' - {st.session_state.project_act_summ.PHASE[i]}') for i in range(len(st.session_state.project_act_summ))]
    index_dict= {'-':'-'}
    for i in range(len(st.session_state.project_act_summ)):
        index_dict[index_sel[i+1]] = i

    if project_sel == '-':
        st.info('SELECT PROJECT TO UPLOAD EVENTS')
    else:
        st.markdown(f"SUMMARY OF TASKS REGISTERED FOR {company_id}'s PROJECT {project_sel}")
        st.write(st.session_state.project_act_summ)

#st.session_state.pre_load_event = pd.DataFrame()
with st.form('event_upload'):
    b01, b02, b03, b04 = st.columns(4)
    with b01:
        act_index = st.selectbox("SELECT TASK INDEX", index_sel)
        #if act_index != '-': #st.write(st.session_state.project_act_summ['l2_id'][index_dict[act_index]])
    with b02:
        event_type = st.selectbox("EVENT TYPE",event_types)
    with b03:
        mitigate_event = st.selectbox('MITIGATED?', ['N', 'Y'])
    with b04:
        weeks_impact = st.number_input("DELAY/IMPACT [weeks]", value=0, step = 1 ,format = '%i')

    event_desc = st.text_area('EVENT DESCTIPTION', max_chars=200)
    mitigation_desc = st.text_area('MITIGATION DETAILS', value = 'None', max_chars=200)

    check_event = st.form_submit_button('CHECK EVENT')
    if check_event:
        if (act_index!= '-') & (event_type != '-') & (event_desc != ''):
            st.session_state.event_id = f'{project_sel}-{int((time.time()))}'
            st.session_state.pre_load_event = pd.DataFrame(data = np.array([st.session_state.project_act_summ['PHASE'][index_dict[act_index]],event_type, event_desc, mitigate_event, mitigation_desc, weeks_impact, st.session_state.event_id, st.session_state.project_act_summ['TASK ID'][index_dict[act_index]]]), index=np.array(['TASK','EVENT TYPE','EVENT DESCR.', 'MITIGATED', 'MITIGATION', 'IMPACT(weeks)', 'EVENT ID', 'TASK ID']))
            st.write(st.session_state.pre_load_event.transpose())

        else:
            if act_index == '-':
                st.warning('Please select TASK INDEX')
            if event_type == '-':
                st.warning('Please select EVENT TYPE')
            if event_desc == '':
                st.warning('Please provide EVENT DESCRIPTION')
pre_load_event = st.button(f'ADD EVENT')# {project_sel}-{int((time.time()))} FOR PROJECT {project_sel}')
if pre_load_event:
    if 'pre_load_reg' not in st.session_state:
        st.session_state.pre_load_reg = st.session_state.pre_load_event.copy().transpose()
        st.session_state.pre_load_reg['index'] = st.session_state.pre_load_reg.index
    else:
        if st.session_state.pre_load_event.copy().transpose()['EVENT ID'][0] in st.session_state.pre_load_reg['EVENT ID'].tolist():
            st.warning(f'Register already pre-loaded. CHECK EVENT. The value "EVENT ID" must be unique (EVENT ID number {st.session_state.event_id} already exists. )')
        else:
            st.session_state.pre_load_reg = st.session_state.pre_load_reg.append(st.session_state.pre_load_event.transpose(), ignore_index=True)
            st.session_state.pre_load_reg['index'] = st.session_state.pre_load_reg.index
st.markdown('***')
st.subheader('2. UPLOAD EVENTS')

del_list = ['-']+st.session_state.pre_load_reg['index'].unique().tolist()
del01, del02 = st.columns((13,2))
with del02:
    delete_reg_num = st.selectbox('INDEX', del_list)
    delete_reg = st.button('DELETE')
if delete_reg:
    if delete_reg_num =='-':
        st.warning(f'Select item index to delete')
    else:
        del_index = st.session_state.pre_load_reg[st.session_state.pre_load_reg['index'] == delete_reg_num].index
        st.session_state.pre_load_reg = st.session_state.pre_load_reg.drop(del_index[0])
        st.session_state.pre_load_reg = st.session_state.pre_load_reg.reset_index(drop = True)
        st.session_state.pre_load_reg['index'] = st.session_state.pre_load_reg.index
with del01:
    if 'pre_load_reg' in st.session_state:
        with st.form('upload_events_reg'):
            st.dataframe(st.session_state.pre_load_reg)
            upload_events_reg = st.form_submit_button('UPLOAD TO DB')
            if upload_events_reg:
                for i in range(len(st.session_state.pre_load_reg)):
                    upload_query = f"INSERT INTO events_main (event_type, event_id, l2_id, event_desc, mitigated, mitigation_details, est_impact) VALUES('{st.session_state.pre_load_reg['EVENT TYPE'][i]}', '{st.session_state.pre_load_reg['EVENT ID'][i]}', '{st.session_state.pre_load_reg['TASK ID'][i]}', '{st.session_state.pre_load_reg['EVENT DESCR.'][i]}', '{st.session_state.pre_load_reg['MITIGATED'][i]}', '{st.session_state.pre_load_reg['MITIGATION'][i]}', '{st.session_state.pre_load_reg['IMPACT(weeks)'][i]}')"
                    run_query(upload_query)
                    st.success(f'Uploaded index {i}, event {st.session_state.pre_load_reg["EVENT ID"][i]} (waiting for review)')
#if
#    st.success('Events registtry uploaded')
        #hollmann@validest.com

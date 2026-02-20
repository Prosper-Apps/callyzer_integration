from __future__ import unicode_literals
import frappe
from frappe import _
from frappe.integrations.utils import create_request_log,make_post_request
from frappe.utils import get_datetime,now_datetime,format_datetime,format_date,format_time,getdate, duration_to_seconds,format_duration, ceil, now
from datetime import timedelta
import datetime
from frappe.utils.password import get_decrypted_password
import json
import re
import time
from frappe.utils.background_jobs import enqueue
from frappe.utils import add_days, getdate, today, get_datetime

def get_callyzer_configuration():
	if frappe.db.get_single_value("Callyzer Settings", "enabled"):
		callyzer_settings = frappe.get_doc("Callyzer Settings")
		return {
			"bearer_token": callyzer_settings.get_password(fieldname="bearer_token", raise_exception=False),
			"last_api_call_time": callyzer_settings.last_api_call_time,
			"call_history_url":callyzer_settings.call_history_url
		}
	return "disabled"

@frappe.whitelist()
def auto_pull_callyzer_logs():
	callyzer_settings=get_callyzer_configuration()
	if callyzer_settings!='disabled':
		fetch_callyzer_data_and_make_integration_request(callyzer_settings)
	return

def call_summary_api_and_create_call_log(url, data, headers, start_time, end_time, page_size):
	request_log_data = {
	    "synced_from" : start_time.timestamp(),
	    "synced_to" : end_time.timestamp(),
	    "page_no":1,
	    "page_size" : page_size,
	    "reference_doctype":"Callyzer Call Log"
	}
 
	integration_request = create_request_log(
		data = frappe._dict(request_log_data),
		integration_type = "Remote",
		service_name = "Callyzer"
	)

	recordsTotal = 0
	try:
		response = make_post_request(url, headers=headers, data=data)
		frappe.db.set_value('Integration Request', integration_request.name, 'output',json.dumps(response))
	
		output=frappe.db.get_value('Integration Request', integration_request.name, 'output')
		if not output:
			frappe.db.set_value('Integration Request', integration_request.name, 'status', 'Failed')
		else:
			# Iterate through response	
			recordsTotal=frappe._dict(response).get('total_records')
			if recordsTotal!=0:
				for key ,value in frappe._dict(response).items():
					if key == 'result':
						for call_row in value:
							callyzer_call_log = make_callyzer_call_log_records(call_row, integration_request.name)
		frappe.db.set_value('Integration Request', integration_request.name, 'status', 'Completed')
		return response, recordsTotal
	
	except Exception as e:
		print(e)
		frappe.db.set_value('Integration Request', integration_request.name, 'status', 'Failed')
		if hasattr(e, 'response'):
			frappe.log_error(title='Callyzer Error', message=frappe.get_traceback()+'\n\n\n'+json.dumps(data)+'\n\n\n'+e.response.text)
		else:
			frappe.log_error(title='Callyzer Error', message=frappe.get_traceback()+'\n\n\n'+json.dumps(data))
			# Set Last Time, As Error Is Not With API But In Data
			frappe.db.set_value('Callyzer Settings','Callyzer Settings', 'last_api_call_time', end_time)
		return {}, recordsTotal

def fetch_callyzer_data_and_make_integration_request(callyzer_settings):
	try:
		# Prepare Post Data To Pass In Headers and Payload
		sleep_time = 2
		page_size = 100
		url = callyzer_settings.get('call_history_url')
		headers={
			"Authorization": "Bearer {0}".format(callyzer_settings.get('bearer_token'), raise_exception=False),
			'Content-Type': 'application/json'
		}
		
		current_datetime = datetime.datetime.now()
		if callyzer_settings.get('last_api_call_time'):
			# Get Data From Last API Call Time - 3 Minutes To Ensure That No Call History Will Left.
			start_time = get_datetime(callyzer_settings.get('last_api_call_time'))- datetime.timedelta(minutes=3)
			end_time = current_datetime
		else:
			# If Cannot Find Last API Call Time - Take Start Time As Current Time - 5 Min
			start_time = current_datetime - datetime.timedelta(minutes=5)
			end_time = current_datetime

		data=json.dumps({
			"call_from" : start_time.timestamp(),
			"call_to" : end_time.timestamp(),
			"page_no":1,
			"page_size" : page_size        
		})
		print(data)
		response, recordsTotal = call_summary_api_and_create_call_log(url, data, headers, start_time, end_time, page_size)
		print(response)
		# If All Data Is Not Covered Then Call Again For Next Page
		if response != {} and (len(frappe._dict(response).get('result')) < recordsTotal):
			time.sleep(sleep_time)
			no_of_iteration = ceil(recordsTotal / page_size)
			if no_of_iteration > 0:
				for i in range(2, no_of_iteration+1):
					data=json.dumps({
						"call_from" : start_time.timestamp(),
						"call_to" : end_time.timestamp(),
						"page_no":i,
						"page_size" : page_size        
					})
					response, recordsTotal = call_summary_api_and_create_call_log(url, data, headers, start_time, end_time, page_size)
					time.sleep(sleep_time)
				frappe.db.set_value('Callyzer Settings','Callyzer Settings', 'last_api_call_time', end_time)
					
	except Exception as e:
		if hasattr(e, 'response'):
			frappe.log_error(title='Callyzer Error', message=frappe.get_traceback()+'\n\n\n'+json.dumps(data)+'\n\n\n'+e.response.text)
		else:
			frappe.log_error(title='Callyzer Error', message=frappe.get_traceback()+'\n\n\n'+json.dumps(data))
		return		

def make_callyzer_call_log_records(call_row, integration_request):
	unique_id = call_row.get('id')
	if not frappe.db.exists("Callyzer Call Log", {"unique_call_id": unique_id}):
		print("creating....")
		call_log = frappe.new_doc('Callyzer Call Log')
		call_log.client = call_row.get('client_name')
		call_log.client_country_code = call_row.get('client_country_code')
		call_log.customer_mobile = call_row.get('client_number')
		call_log.employee = call_row.get('emp_name')
		call_log.employee_country_code = call_row.get('emp_country_code')
		call_log.employee_tags = str(call_row.get('emp_tags'))
		call_log.employee_mobile = call_row.get('emp_number')
		call_log.unique_call_id = call_row.get('id')
		call_log.calltype = call_row.get('call_type')
		call_log.date = call_row.get('call_date')
		call_log.time = call_row.get('call_time')
		call_log.duration = call_row.get('duration')
		call_log.note = call_row.get('note')
		call_log.call_recording_url = call_row.get('call_recording_url')
		call_log.synced_at = call_row.get('synced_at')
		call_log.crm_status = call_row.get('crm_status')
		call_log.reminder_date = call_row.get('reminder_date')
		call_log.reminder_time = call_row.get('reminder_time')
		call_log.lead_id = call_row.get('lead_id')
		call_log.integration_request = integration_request
		call_log.raw_log = json.dumps(call_row, indent=4)

		call_log.save(ignore_permissions = True)
		return call_log.name
	else: 
		# Duplicate Call Record Found...Skip 
		print("Duplicate Call Record Found...Skip ....", frappe.db.exists("Callyzer Call Log", {"unique_call_id": unique_id}))
		pass 
	return	 

@frappe.whitelist()
def load_lead_call_info(self,method):
		if self.mobile_no:	
			call_info = get_call_info(self.mobile_no)
			self.set_onload('call_info', call_info)	

def get_call_info(mobile_no):
	data = frappe.db.sql('''
		SELECT 
			lead.name as lead_name,
			lead.mobile_no as `customer_no`,
			lead.creation as creation,
			TIMESTAMPDIFF(SECOND,lead.creation,min(addtime(call_log.date, call_log.time))) as `first_call_response_time`,
			min(addtime(call_log.date, call_log.time)) as `first_call`, 
			max(addtime(call_log.date, call_log.time)) as `last_call`,
			COUNT(call_log.name) as `total_count`,
			COUNT(CASE WHEN call_log.calltype = 'Outgoing' THEN call_log.name ELSE NULL END) as `outgoing_count`,
			COUNT(CASE WHEN call_log.calltype = 'Incoming' THEN call_log.name ELSE NULL END) as `incoming_count`,
			COUNT(CASE WHEN call_log.calltype = 'Missed' THEN call_log.name ELSE NULL END) as `missed_count`,
			COUNT(CASE WHEN call_log.calltype = 'Rejected' THEN call_log.name ELSE NULL END) as `rejected_count`
		FROM  
			`tabCallyzer Call Log` call_log
		INNER JOIN 
			`tabLead` lead
		ON 
			call_log.customer_mobile = lead.mobile_no 
		WHERE 
			lead.mobile_no=%s
		GROUP BY 
			lead.mobile_no
		''', (mobile_no),as_dict=True)
	result = data[0] if data else None
	if result:
		if 'first_call_response_time' in result:
			result['first_call_response_time']=format_duration(result['first_call_response_time'])
	return  result

@frappe.whitelist()
def fetch_last_thirty_days_connected_calls_in_lead():
	leads = frappe.db.get_all(
		doctype = "Lead",
		filters = {
			"status": ["not in", ["Converted"]],
		},
		fields = ['name', 'mobile_no']
	)
	print("Total Leads: ", len(leads))
	if leads:
		for lead in leads:
			end_date = add_days(getdate(today()), -1)
			start_date = add_days(getdate(end_date), -30)

			count = frappe.db.sql('''
				SELECT 
					COUNT(name) as total_connected
				FROM  
					`tabCallyzer Call Log` call_log
				WHERE
					(call_log.calltype = 'Outgoing' OR call_log.calltype = 'Incoming')
				AND 
					call_log.duration > 2
				AND 
					call_log.customer_mobile = "{0}"
				AND 
					call_log.date BETWEEN "{1}" AND "{2}"
			'''.format(lead.mobile_no, start_date, end_date)
			, as_dict = 1)

			last_call_detail = frappe.db.sql('''
				SELECT 
					call_log.`date` as last_date, 
					call_log.calltype as type, 
					call_log.time,
					call_log.duration 
				FROM  
					`tabCallyzer Call Log` call_log
				WHERE
					call_log.customer_mobile = "{0}"
				ORDER BY call_log.`date` DESC, call_log.time  DESC
				LIMIT 1
			'''.format(lead.mobile_no)
			, as_dict = 1)

			if len(count) > 0 and len(last_call_detail) > 0:
				print(lead, count)
				# doc = frappe.get_doc("Lead", lead['name'])
				# doc.custom_last_thirty_days_call_count = count[0].total_connected
				# doc.save(ignore_permissions=True)
				frappe.db.set_value("Lead", lead.get("name"), "custom_last_30_days_call_count", count[0].total_connected)
				frappe.db.set_value("Lead", lead.get("name"), "custom_last_call_date", last_call_detail[0].last_date)
				outcome = ""
				if last_call_detail[0].type == "Outgoing" and last_call_detail[0].duration > 2:
					outcome = "Connected"
				if last_call_detail[0].type == "Outgoing" and last_call_detail[0].duration < 2:
					outcome = "Not Connected"
				elif last_call_detail[0].type == "Rejected":
					outcome = "Not Connected"
				elif last_call_detail[0].type == "Incoming":
					outcome = "Incoming"
				elif last_call_detail[0].type == "Missed":
					outcome = "Missed"

				frappe.db.set_value("Lead", lead.get("name"), "custom_call_outcome", outcome)
				frappe.db.commit()
				print('Updated')
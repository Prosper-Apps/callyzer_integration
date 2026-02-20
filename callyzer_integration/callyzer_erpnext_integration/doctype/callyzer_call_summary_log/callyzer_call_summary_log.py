# Copyright (c) 2026, GreyCube Technologies and contributors
# For license information, please see license.txt

import time
import frappe
import json
import requests
from frappe.model.document import Document
from frappe.integrations.utils import create_request_log
from frappe.utils import add_days, getdate, today, get_datetime

class CallyzerCallSummaryLog(Document):
	pass

def make_api_request(method, url, headers = None, payload = None):
	error, result = None, {}
	try:
		response = requests.request(
			method = method,
			url = url,
			headers = headers,
			json = payload,
		)

		if response.status_code == 200:
			result = response.json()
		else:
			error = response.json()
	except Exception:
		error = frappe.get_traceback()

	return result, error

def get_callyzer_configuration():
	if frappe.db.get_single_value("Callyzer Settings", "enabled"):
		callyzer_settings = frappe.get_doc("Callyzer Settings")
		return {
			"bearer_token": callyzer_settings.get_password(fieldname="bearer_token", raise_exception=False),
			"callyzer_summary_log_url":callyzer_settings.callyzer_summary_log_url,
			"last_summary_api_call_date": callyzer_settings.last_summary_api_call_date
		}
	return "disabled"

def create_summary_log(call_date, sales_person, res):
	if not frappe.db.exists("Callyzer Call Summary Log", {"call_summary_date": call_date, "sales_person_mobile_no": sales_person.mobile_no_cf}):
		doc = frappe.new_doc("Callyzer Call Summary Log")
		doc.sales_person = sales_person.name
		doc.sales_person_mobile_no = sales_person.mobile_no_cf
		doc.call_summary_date = call_date
		doc.total_incoming_calls = res.get("result").get("total_incoming_calls")
		doc.total_incoming_duration = res.get("result").get("total_incoming_duration")
		doc.total_outgoing_calls = res.get("result").get("total_outgoing_calls")
		doc.total_outgoing_duration = res.get("result").get("total_outgoing_duration")
		doc.total_missed_calls = res.get("result").get("total_missed_calls")
		doc.total_calls = res.get("result").get("total_calls")
		doc.total_rejected_calls = res.get("result").get("total_rejected_calls")
		doc.total_never_attended_calls = res.get("result").get("total_never_attended_calls")
		doc.total_duration = res.get("result").get("total_duration")
		doc.total_unique_clients = res.get("result").get("total_unique_clients")
		doc.total_not_pickup_by_client_calls = res.get("result").get("total_not_pickup_by_clients_calls")
		doc.total_connected_calls = res.get("result").get("total_connected_calls")
		doc.total_working_hours = res.get("result").get("total_working_hours")

		doc.save(ignore_permissions=True)

@frappe.whitelist()
def fetch_per_day_call_summary():
	sales_persons = frappe.db.get_all(
		doctype = "Sales Person",
		filters = {
			"enabled" : 1,
			"is_group": 0
		},
		fields = ["name", "mobile_no_cf"]
	)

	settings = get_callyzer_configuration()
	if settings != "disabled":
		if settings.get('last_summary_api_call_date'):
			call_date = add_days(getdate(settings.get('last_summary_api_call_date')), +1)
		else:
			call_date = add_days(getdate(today()), -1)

		starting_timestamp = get_datetime(f"{call_date} 01:00:00").timestamp()
		ending_timestamp = get_datetime(f"{call_date} 23:30:00").timestamp()
		
		headers = {
			'Authorization': 'Bearer {0}'.format(settings.get('bearer_token')),
			'Content-Type': 'application/json'
		}

		payload = {
			"call_from": starting_timestamp,
			"call_to": ending_timestamp,
			"call_types": ["Missed","Rejected","Incoming","Outgoing"],
			"is_exclude_numbers": True,
			"page_no": 1,
			"page_size": 100
		}

		if sales_persons and len(sales_persons) > 0:
			for sp in sales_persons:
				if sp.mobile_no_cf != '-' and sp.mobile_no_cf != None:
					request_log_data = {
						"call_date": call_date,
						"call_from": starting_timestamp,
						"call_to": ending_timestamp,
						"sales_person": sp,
						"reference_doctype":"Callyzer Call Summary Log"
					}
				
					integration_request = create_request_log(
						data = frappe._dict(request_log_data),
						integration_type = "Remote",
						service_name = "Callyzer"
					)

					payload["emp_numbers"] = [sp.mobile_no_cf]
					res, err = make_api_request("POST", settings.get('callyzer_summary_log_url'), headers, payload)
					if res: 
						create_summary_log(call_date=call_date, sales_person=sp, res=res)
						frappe.db.set_value('Integration Request', integration_request.name, 'output', json.dumps(res, indent=4))
						frappe.db.set_value('Integration Request', integration_request.name, 'status', 'Completed')
					elif err: 
						frappe.log_error(
							title = "Callyzer Call Summary Log API Failed",
							message = err
						)
						frappe.db.set_value('Integration Request', integration_request.name, 'error', json.dumps(err, indent=4))
						frappe.db.set_value('Integration Request', integration_request.name, 'status', 'Failed')
					time.sleep(2)
			frappe.db.set_value('Callyzer Settings','Callyzer Settings', 'last_summary_api_call_date', add_days(getdate(today()), -1))

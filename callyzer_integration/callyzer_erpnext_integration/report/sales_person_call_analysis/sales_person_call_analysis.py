# Copyright (c) 2013, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe import _, scrub

def execute(filters=None):
	if not filters: filters = {}
	columns = get_columns(filters)
	data = get_data(filters)
	return columns, data

def get_conditions(filters) :
	conditions = []
	if filters.get("sales_person"):
		conditions.append(" and sales.sales_person_name=%(sales_person)s")
	return " ".join(conditions) if conditions else ""

def get_columns(filters):
	columns = [
		{
			'label': _('Sales Person Name'),
			'options': 'Sales Person',
			'fieldname': 'sales_person',
			'fieldtype': 'Link',
			'width': 300
		},
		{
			'fieldname': 'date',
			'label': _('Date'),
			'fieldtype': 'Date',
			'width': 120
		},
		{
			'fieldname': 'first_call',
			'label': _('First Call Time'),
			'fieldtype': 'Time',
			'width': 80
		},	
		{
			'fieldname': 'last_call',
			'label': _('Last Call Time'),
			'fieldtype': 'Time',
			'width': 80
		},		
		{
			'fieldname': 'outgoing_calls',
			'label': _('Outgoing Calls'),
			'fieldtype': 'Int',
			'width': 150
		},		
		{
			'fieldname': 'incoming_calls',
			'label': _('Incoming Calls'),
			'fieldtype': 'Int',
			'width': 150
		},		
		{
			'fieldname': 'missed_calls',
			'label': _('Missed Calls'),
			'fieldtype': 'Int',
			'width': 150
		},	
		{
			'fieldname': 'rejected_calls',
			'label': _('Rejected Calls'),
			'fieldtype': 'Int',
			'width': 150
		},
		{
			'fieldname': 'never_attended',
			'label': _('Never Attended Calls'),
			'fieldtype': 'Int',
			'width': 150
		},
		{
			'fieldname': 'connected_leads',
			'label': _('Connected - Lead'),
			'fieldtype': 'Int',
			'width': 150
		},
		{
			'fieldname': 'connected_customers',
			'label': _('Connected - Customer'),
			'fieldtype': 'Int',
			'width': 150
		},
		{
			'fieldname': 'connected_internal',
			'label': _('Connected - Internal'),
			'fieldtype': 'Int',
			'width': 150
		},	
		{
			'fieldname': 'connected_others',
			'label': _('Connected - Others'),
			'fieldtype': 'Int',
			'width': 150
		},
		{
			'fieldname': 'total_connected_calls',
			'label': _('Total Connected Calls'),
			'fieldtype': 'Int',
			'width': 150
		},	
		{
			'fieldname': 'total_unique_clients',
			'label': _('Total Unique Clients'),
			'fieldtype': 'Int',
			'width': 150
		},
		{
			'fieldname': 'total_never_attended_calls',
			'label': _('Total Never Attended Calls'),
			'fieldtype': 'Int',
			'width': 150
		},
		{
			'fieldname': 'total_not_pickup_by_clients_calls',
			'label': _('Total Not Pickup By Client Calls'),
			'fieldtype': 'Int',
			'width': 150
		},				
	]
	return columns

def get_data(filters):
	customer = frappe.db.sql('''
			SELECT 
				tc.phone
			FROM
				`tabContact` tc
			INNER JOIN
				`tabDynamic Link` tdl
			ON 
				tdl.parent = tc.name
			WHERE
				tdl.link_doctype = "Customer"
		''', as_dict = 1)
	customer_nos = []
	for cust in customer:
		customer_nos.append(cust.phone)	

	employees = frappe.db.sql(
		'''
		SELECT e.cell_number
		FROM `tabEmployee` e
		WHERE e.status = "Active" AND e.cell_number IS NOT NULL
		'''
	, as_dict = 1)
	employee_nos = []
	for emp in employees:
		employee_nos.append(emp.cell_number)

	leads = frappe.db.sql(
		'''
		SELECT lead.mobile_no
		FROM `tabLead` lead
		WHERE lead.status = "Converted" AND lead.mobile_no IS NOT NULL
		'''
	, as_dict = 1)
	lead_nos = []
	for lead in leads:
		lead_nos.append(lead.mobile_no)

	all_nos = customer_nos + lead_nos + employee_nos

	data = frappe.db.sql("""
		select 
			sales.sales_person_name as sales_person, 
			call_log.employee_mobile,
			call_log.date, 
			MIN(call_log.`time`) as first_call,
			MAX(call_log.`time`) as last_call,
			COUNT(CASE WHEN call_log.calltype = 'Outgoing' THEN call_log.name ELSE NULL END) as outgoing_calls,
			COUNT(CASE WHEN call_log.calltype = 'Incoming' THEN call_log.name ELSE NULL END) as incoming_calls,
			COUNT(CASE WHEN call_log.calltype = 'Missed' THEN call_log.name ELSE NULL END) as missed_calls,
			COUNT(CASE WHEN call_log.calltype = 'Rejected' THEN call_log.name ELSE NULL END) as rejected_calls,
			COUNT(CASE WHEN call_log.customer_mobile IN {customers} THEN call_log.name ELSE NULL END) as connected_customers,
			COUNT(CASE WHEN call_log.customer_mobile IN {emps} THEN call_log.name ELSE NULL END) as connected_internal,
			COUNT(CASE WHEN call_log.customer_mobile IN {leads} THEN call_log.name ELSE NULL END) as connected_leads,
			COUNT(CASE WHEN call_log.customer_mobile NOT IN {all_nos} THEN call_log.name ELSE NULL END) as connected_others,
			IFNULL(summary_log.total_connected_calls, 0) as total_connected_calls,
			IFNULL(summary_log.total_unique_clients, 0) as total_unique_clients,
			IFNULL(summary_log.total_not_pickup_by_client_calls, 0) as total_not_pickup_by_clients_calls,
			IFNULL(summary_log.total_never_attended_calls, 0) as total_never_attended_calls,
			IFNULL(summary_log.total_never_attended_calls, 0) as never_attended
		FROM  
			`tabCallyzer Call Log` call_log
		INNER JOIN 
			`tabSales Person` sales
		ON 
			call_log.employee_mobile = sales.mobile_no_cf 
		LEFT OUTER JOIN
			`tabCallyzer Call Summary Log` summary_log
		ON 
			call_log.employee_mobile = summary_log.sales_person_mobile_no
		AND 
			call_log.date = summary_log.call_summary_date
		where date(call_log.date) between %(from_date)s AND %(to_date)s
		{conditions}
		group by call_log.date,sales.mobile_no_cf 
		order by call_log.date desc,sales.mobile_no_cf asc""".format(conditions=get_conditions(filters), customers = tuple(customer_nos), emps = tuple(employee_nos), leads = tuple(lead_nos), all_nos = tuple(all_nos)), filters, as_dict=1)
	
	return data
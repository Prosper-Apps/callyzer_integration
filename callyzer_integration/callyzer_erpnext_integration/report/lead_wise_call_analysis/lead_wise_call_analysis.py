# Copyright (c) 2013, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt

from __future__ import unicode_literals
import frappe
from frappe import _, scrub

def execute(filters=None):
	if not filters: filters = {}
	columns = get_columns(filters)
	data = get_data(filters)
	if not data:
		frappe.msgprint("No Data Found", alert=True)
	return columns, data

def get_conditions(filters) :
	conditions = []
	if filters.get("lead_owner"):
		conditions.append(" and lead.lead_owner=%(lead_owner)s")
	return " ".join(conditions) if conditions else ""

def get_columns(filters):
	columns = [
		{
			'label': _('Lead ID'),
			'options': 'Lead',
			'fieldname': 'lead_id',
			'fieldtype': 'Link',
			'width': 130
		},
		{
			"label": _("Lead Name"),
			"fieldname": "lead_name",
			"fieldtype": "Data",
			"width": 160
		},
		{
			'label': _('Lead Owner'),
			'fieldname': 'lead_owner',
			'fieldtype': 'Data',
			'width': 150
		},
		{
			'label': _('Lead Stage'),
			'fieldname': 'lead_stage',
			'fieldtype': 'Data',
			'width': 90
		},
		{
			"fieldname":"status",
			"label": _("Lead Status"),
			"fieldtype": "Data",
			"width": 90
		},	
		{
			'fieldname': 'lead_created_on',
			'label': _('Lead Created On'),
			'fieldtype': 'Date',
			'width': 110
		},	
		{
			'fieldname': 'lead_converted_on',
			'label': _('Lead Converted On'),
			'fieldtype': 'Date',
			'width': 110
		},	
		{
			'fieldname': 'first_call_response_time',
			'label': _('First Response Time'),
			'fieldtype': 'Duration',
			'width': 180
		},
				{
			'fieldname': 'last_call_date_time',
			'label': _('Last Call Date & Time'),
			'fieldtype': 'Datetime',
			'width': 180
		},	
		{
			"label": _("Mobile"),
			"fieldname": "mobile_no",
			"fieldtype": "Data",
			"width": 110
		},									
		{
			'fieldname': 'first_call',
			'label': _('First Call'),
			'fieldtype': 'Time',
			'width': 80
		},				
		{
			'fieldname': 'total_calls',
			'label': _('Total Calls'),
			'fieldtype': 'Int',
			'width': 65
		},
		{
			'fieldname': 'total_connected_calls',
			'label': _('Total Connected Calls'),
			'fieldtype': 'Int',
			'width': 65
		},	
		{
			'fieldname': 'connected_last_thirty_days_calls',
			'label': _('Connected Calls - Last 30Days'),
			'fieldtype': 'Int',
			'width': 65
		},		
		{
			'fieldname': 'outgoing_calls',
			'label': _('Outgoing Calls'),
			'fieldtype': 'Int',
			'width': 90
		},		
	
		{
			'fieldname': 'incoming_calls',
			'label': _('Incoming Calls'),
			'fieldtype': 'Int',
			'width': 90
		},		
		{
			'fieldname': 'missed_calls',
			'label': _('Missed Calls'),
			'fieldtype': 'Int',
			'width': 80
		},	
		{
			'fieldname': 'rejected_calls',
			'label': _('Rejected Calls'),
			'fieldtype': 'Int',
			'width': 90
		}						
	]
	return columns

def get_data(filters):
	data = frappe.db.sql('''
		SELECT 
			lead.name as lead_id,
			user.full_name as lead_owner,
			lead.stage_cf as lead_stage,
			lead.status,
			lead.creation as lead_created_on, 
			customer.creation as lead_converted_on, 
			lead.lead_name,
			lead.mobile_no,
			lead.custom_last_thirty_days_call_count as connected_last_thirty_days_calls,
			TIMESTAMPDIFF(SECOND,lead.creation,min(addtime(call_log.date, call_log.time))) as `first_call_response_time`,
			MIN(call_log.time) as first_call,
			CONCAT(MAX(call_log.date), ' ',MAX(call_log.`time`)) as last_call_date_time, 
			COUNT(call_log.name) as total_calls,
			COUNT(CASE WHEN call_log.calltype = 'Outgoing' THEN call_log.name ELSE NULL END) as outgoing_calls,
			COUNT(CASE WHEN call_log.calltype = 'Incoming' THEN call_log.name ELSE NULL END) as incoming_calls,
			COUNT(CASE WHEN call_log.calltype = 'Missed' THEN call_log.name ELSE NULL END) as missed_calls,
			COUNT(CASE WHEN call_log.calltype = 'Rejected' THEN call_log.name ELSE NULL END) as rejected_calls,
			COUNT(CASE WHEN call_log.duration > '2s' THEN call_log.name ELSE NULL END) as total_connected_calls
		FROM  
			`tabCallyzer Call Log` call_log
		INNER JOIN 
			`tabLead` lead
		ON 
			call_log.customer_mobile = lead.mobile_no 
		INNER JOIN
			`tabUser` user
		ON 
			user.name = lead.lead_owner
		LEFT OUTER JOIN 
			`tabCustomer` customer
		ON 
			lead.name = customer.lead_name 
		WHERE 
			date(call_log.date) BETWEEN %(from_date)s AND %(to_date)s
			{conditions}
		GROUP BY lead.mobile_no
		ORDER BY call_log.creation desc
	'''.format(conditions=get_conditions(filters)), 
		filters, 
		as_dict=1
	)

	return data
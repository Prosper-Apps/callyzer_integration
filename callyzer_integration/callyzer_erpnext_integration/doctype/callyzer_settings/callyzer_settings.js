// Copyright (c) 2021, GreyCube Technologies and contributors
// For license information, please see license.txt

frappe.ui.form.on('Callyzer Settings', {
	manual_summary_call: function (frm) {
		if (frm.is_dirty()) {
			frappe.throw("Please save the form...")
		}
		frappe.call("callyzer_integration.callyzer_erpnext_integration.doctype.callyzer_call_summary_log.callyzer_call_summary_log.fetch_per_day_call_summary")
		frappe.msgprint("Manual call for Summary API is started, You can check details in Integration Request Doctype")
	},

	manual_call_history: function (frm) {
		if (frm.is_dirty()) {
			frappe.throw("Please save the form...")
		}
		frappe.call("callyzer_integration.callyzer_integration_hook.auto_pull_callyzer_logs")
		frappe.msgprint("Manual call for History API is started, You can check details in Integration Request Doctype")
	}
});

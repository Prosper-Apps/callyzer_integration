import frappe

def after_migrate(**args):
    callyzer_integration_create_custom_fields(**args)

def callyzer_integration_create_custom_fields(**args):
    from frappe.custom.doctype.custom_field.custom_field import create_custom_fields

    custom_fields = {
        "Sales Person": [
            dict(
                doctype="Custom Field",
                dt="Sales Person",
                fieldname="mobile_no_cf",
                fieldtype="Data",
                insert_after="cb0",
                label="Sales Person Mobile",
                name="Sales Person-mobile_no_cf",
                options="Phone",
                reqd=1,
            )
        ],
        "Lead": [
            # dict(
            #     doctype="Custom Field",
            #     dt="Lead",
            #     fieldname="call_info_cf",
            #     fieldtype="HTML",
            #     insert_after="custom_note",
            #     label="Call Info",
            #     name="Lead-call_info_cf",
            # ),
            # dict(
            #     depends_on="eval:doc.mobile_no",
            #     doctype="Custom Field",
            #     dt="Lead",
            #     fieldname="check_call_log_cf",
            #     fieldtype="Button",
            #     insert_after="call_info_cf",
            #     label="Check Call Log",
            #     name="Lead-check_call_log_cf",
            #     permlevel=1
            # ),
            # dict(
            #     fieldname="custom_last_thirty_days_call_count",
            #     fieldtype="Int",
            #     insert_after="custom_primary_objection_if_any",
            #     label="Last 30 Days Call Count",
            #     is_system_generated=0,
            #     is_custom_field=1
            # ),
        ],
    }

    print("Adding custom field in Lead, Sales Person.....")
    create_custom_fields(custom_fields)
    frappe.db.commit()  # to avoid implicit-commit errors

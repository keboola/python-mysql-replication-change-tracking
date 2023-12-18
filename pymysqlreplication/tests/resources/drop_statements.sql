
alter table f_transaction drop billing_type, algorithm=inplace, lock=none
;
alter table payment_method drop plaid_id, drop plaid_institution_type, algorithm=inplace, lock=none
;
alter table `employee_settings` drop `new_lever_id`;

alter table employee_settings drop new_lever_id, algorithm=inplace, lock=none
;
alter table store alter column shopify_do_not_change_fulfillment_service drop default, lock=none
;
alter table employee_settings drop lever_id, algorithm=inplace, lock=none
;
alter table return_entity drop returnly_id, drop returnly_status, algorithm=inplace, lock=none
;
alter table product drop lot_control_enabled, algorithm=inplace, lock=none
;
alter table store drop fulfillment_countries, algorithm=inplace, lock=none
;
 alter table warehouse_config              drop pitney_bowes_carrier_facility_id,              drop pitney_bowes_client_facility_id,              drop pitney_bowes_client_id,              drop pitney_bowes_shipper_id,              algorithm=inplace,              lock=none
;

alter table order_fee_breakdown drop index uniq_active__order__category, algorithm=inplace, lock=none
;
alter table product drop barcodes, algorithm=inplace, lock=none
;
alter table sales_prediction_adjusted drop is_main_model, algorithm=inplace, lock=none
;
ALTER TABLE user DROP zenefits_id, DROP paylocity_id, ALGORITHM=INPLACE, LOCK=NONE
;
ALTER TABLE tracker DROP date_time_zone_fixed, ALGORITHM=INPLACE, LOCK=NONE
;
ALTER TABLE claim DROP createdAt, ALGORITHM=INPLACE, LOCK=NONE
;
ALTER TABLE warehouse_config DROP problem_solving_unpick_enabled, ALGORITHM=INPLACE, LOCK=NONE
;
ALTER TABLE warehouse_config DROP inventory_adjustment_research_enabled, ALGORITHM=INPLACE, LOCK=NONE
;
ALTER TABLE pick DROP picking_session_id, ALGORITHM=INPLACE, LOCK=NONE
;
ALTER TABLE pick DROP picking_session_id, ALGORITHM=INPLACE, LOCK=NONE
;
ALTER TABLE item DROP product_count, ALGORITHM=INPLACE, LOCK=NONE
;
ALTER TABLE mobile_app_log DROP updated_at, ALGORITHM=INPLACE, LOCK=NONE
;
ALTER TABLE occurrence DROP checked, ALGORITHM=INPLACE, LOCK=NONE
;
ALTER TABLE picking_tote_assignment DROP problem_code, ALGORITHM=INPLACE, LOCK=NONE;
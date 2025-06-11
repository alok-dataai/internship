#!/bin/bash

PROJECT_ID="playground-s-11-1a274b22"
INSTANCE_ID="spanner-instance-or"
DATABASE_ID="telecom-db"

echo "Creating Spanner instance..."
gcloud spanner instances create $INSTANCE_ID \
    --project=$PROJECT_ID \
    --config=regional-us-central1 \
    --description="Spanner Instance" \
    --processing-units=100



echo "Waiting for Spanner instance to be fully provisioned..."
sleep 10  

echo "Creating Spanner database..."
gcloud spanner databases create $DATABASE_ID --instance=$INSTANCE_ID



DDL_COMMANDS="
CREATE TABLE Customers (
    customer_id STRING(30) NOT NULL,
    first_name STRING(20),
    last_name STRING(20),
    date_of_birth DATE,
    gender STRING(10),
    email STRING(50),
    phone STRING(30)
) PRIMARY KEY (customer_id);

CREATE TABLE Address (
    address_id STRING(30) NOT NULL,
    customer_id STRING(30) NOT NULL,
    street STRING(50),
    city STRING(30),
    state STRING(20),
    postal_code STRING(10),
    country STRING(20),
    CONSTRAINT fk_address_customer FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
) PRIMARY KEY (customer_id, address_id),
  INTERLEAVE IN PARENT Customers ON DELETE CASCADE;

CREATE TABLE Accounts (
    account_id STRING(30) NOT NULL,
    customer_id STRING(30) NOT NULL,
    status STRING(10),
    start_date DATE,
    balance FLOAT64,
    last_payment_date DATE,
    CONSTRAINT fk_accounts_customers FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
) PRIMARY KEY (customer_id, account_id),
  INTERLEAVE IN PARENT Customers ON DELETE CASCADE;

CREATE TABLE Plans (
    customer_id STRING(30) NOT NULL,
    account_id STRING(30) NOT NULL,
    plan_id STRING(30) NOT NULL,
    plan_name STRING(20),
    plan_type STRING(20),
    price_per_month FLOAT64,
    data_limit_gb INT64,
    call_minutes INT64,
    sms_limit INT64,
    CONSTRAINT fk_accounts_plan FOREIGN KEY (customer_id, account_id) REFERENCES Accounts(customer_id, account_id)
) PRIMARY KEY (customer_id, account_id, plan_id),
  INTERLEAVE IN PARENT Accounts ON DELETE CASCADE;

  CREATE TABLE Usage (
    customer_id STRING(30) NOT NULL,
    account_id STRING(30) NOT NULL,
    plan_id STRING(30) NOT NULL,
    period DATE NOT NULL,
    data_used_gb FLOAT64,
    call_minutes_spent INT64,
    sms_sent INT64,
    CONSTRAINT fk_usage_plan FOREIGN KEY (customer_id, account_id, plan_id) REFERENCES Plans(customer_id, account_id, plan_id)
) PRIMARY KEY (customer_id, account_id, plan_id, period),
  INTERLEAVE IN PARENT Plans ON DELETE CASCADE;

CREATE TABLE Billing (
    customer_id STRING(30) NOT NULL,
    account_id STRING(30) NOT NULL,
    billing_id STRING(30) NOT NULL,
    billing_period DATE,
    total_amount FLOAT64,
    due_date DATE,
    payment_status STRING(20),
    payment_method STRING(20),
    CONSTRAINT fk_billing_accounts FOREIGN KEY (customer_id, account_id) REFERENCES Accounts(customer_id, account_id)
) PRIMARY KEY (customer_id, account_id, billing_id),
  INTERLEAVE IN PARENT Accounts ON DELETE CASCADE;

  CREATE TABLE Devices (
    customer_id STRING(30) NOT NULL,
    account_id STRING(30) NOT NULL,
    device_id STRING(30) NOT NULL,
    brand STRING(20),
    model STRING(20),
    sim_number STRING(30),
    status STRING(10),
    CONSTRAINT fk_devices_accounts FOREIGN KEY (customer_id, account_id) REFERENCES Accounts(customer_id, account_id)
) PRIMARY KEY (customer_id, account_id, device_id),
  INTERLEAVE IN PARENT Accounts ON DELETE CASCADE;

CREATE TABLE Tickets (
    customer_id STRING(30) NOT NULL,
    account_id STRING(30) NOT NULL,
    ticket_id STRING(30) NOT NULL,
    device_id STRING(30),
    issue_type STRING(20),
    description STRING(100),
    status STRING(20),
    created_at DATE,
    resolved_at DATE,
    CONSTRAINT fk_ticket_account FOREIGN KEY (customer_id, account_id) REFERENCES Accounts(customer_id, account_id),
    CONSTRAINT fk_ticket_device FOREIGN KEY (device_id) REFERENCES Devices(device_id)
) PRIMARY KEY (customer_id, account_id, ticket_id),
  INTERLEAVE IN PARENT Accounts ON DELETE CASCADE;
"


gcloud spanner databases ddl update $DATABASE_ID --instance=$INSTANCE_ID --ddl="$DDL_COMMANDS"

echo "Tables created successfully!"


# Enabling change streams for each table
CHANGE_STREAMS_COMMANDS="CREATE CHANGE STREAM change_stream_telecom FOR ALL;"

gcloud spanner databases ddl update $DATABASE_ID --instance=$INSTANCE_ID --ddl="$CHANGE_STREAMS_COMMANDS"

echo "Change streams enabled for all tables!"

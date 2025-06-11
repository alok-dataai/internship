package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.Timestamp;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import java.lang.reflect.Type;
import java.util.Map;

public class SpannerChangeStreamToBigQuery {

    public interface Options extends PipelineOptions {
        @Description("GCP project ID")
        String getProjectId();
        void setProjectId(String value);

        @Description("Spanner instance ID")
        String getInstanceId();
        void setInstanceId(String value);

        @Description("Spanner database ID")
        String getDatabaseId();
        void setDatabaseId(String value);

        @Description("Spanner change stream name")
        String getChangeStreamName();
        void setChangeStreamName(String value);

        @Description("BigQuery dataset")
        String getBigQueryDataset();
        void setBigQueryDataset(String value);
    }

    private static final Gson gson = new Gson();
    private static final Type mapType = new TypeToken<Map<String, Object>>(){}.getType();


    public static class CustomDynamicDestinations extends DynamicDestinations<TableRow, String> {
        private final String projectId;
        private final String dataset;

        public CustomDynamicDestinations(String projectId, String dataset) {
            this.projectId = projectId;
            this.dataset = dataset;
        }

        private String normalizeTableName(String spannerTableName) {
            switch (spannerTableName.toLowerCase()) {
                case "customers": return "customers_changes";
                case "address": return "address_changes";
                case "plans": return "plans_changes";
                case "usage": return "usage_changes";
                case "accounts": return "accounts_changes";
                case "billing": return "billing_changes";
                case "devices": return "devices_changes";
                case "tickets": return "tickets_changes";
                default: return spannerTableName.toLowerCase() + "_changes";
            }
        }
        

        @Override
        public String getDestination(ValueInSingleWindow<TableRow> element) {
            return ((String) element.getValue().get("table_name")).toLowerCase();
        }

        @Override
    public TableDestination getTable(String destination) {
    String normalizedTable = normalizeTableName(destination);
    String tableSpec = String.format("%s:%s.%s", projectId, dataset, normalizedTable);
    return new TableDestination(tableSpec, "BigQuery table for " + normalizedTable);
}

        @Override
        public TableSchema getSchema(String destination) {
            return null; 
        }
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setRunner(org.apache.beam.runners.direct.DirectRunner.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<DataChangeRecord> records = pipeline.apply("ReadChangeStream",
            SpannerIO.readChangeStream()
                .withProjectId(options.getProjectId())
                .withInstanceId(options.getInstanceId())
                .withDatabaseId(options.getDatabaseId())
                .withChangeStreamName(options.getChangeStreamName())
                .withInclusiveStartAt(Timestamp.now())
        );

        System.out.println("Change Streams Enabled:");

        PCollection<TableRow> tableRows = records.apply("ParseRecords", ParDo.of(new DoFn<DataChangeRecord, TableRow>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                DataChangeRecord record = c.element();
   		if (record.getTableName().startsWith("change_stream_") || record.getTableName().equalsIgnoreCase("spanner_sys")) {
                    return;
                	}
                for (Mod mod : record.getMods()) {
		    String newValuesJson=mod.getNewValuesJson();

                    // to skip irrelevant data
                    if (newValuesJson== null || newValuesJson.trim().isEmpty()) {
                        return;
                    }

                    Map<String, Object> newData = gson.fromJson(newValuesJson, mapType);
                            if (newData.containsKey("PartitionToken") ||
                            newData.containsKey("State") || 
                            newData.containsKey("ScheduledAt") ||
                            newData.containsKey("RunningAt") ||
                            newData.containsKey("FinishedAt") ||
                            newData.containsKey("Watermark")
                        ) {
                        return; 
                    }
             
                    String tableName = record.getTableName();
                    String modType = record.getModType().name();
                    String timestamp = record.getCommitTimestamp().toString();
        
            
                    Map<String, Object> keys = gson.fromJson(mod.getKeysJson(), mapType);
                    Map<String, Object> data = gson.fromJson(mod.getNewValuesJson(), mapType);
                    // Map<String, Object> data = new java.util.HashMap<>();

                    System.out.println("Keys: " + keys);
                    System.out.println("New Values: " + data);
                  

                    if (keys != null) 
                    {data.putAll(keys);}
                    System.out.println("Data: " + data);
        
                 
        
                    TableRow row = new TableRow();
                    row.set("record_timestamp", timestamp);
                    row.set("mod_type", modType);
                    row.set("table_name", tableName);  
        
                    switch (tableName.toLowerCase()) {
                        case "customers":
                            row.set("customer_id", data.get("customer_id"));
                            row.set("first_name", data.get("first_name"));
                            row.set("last_name", data.get("last_name"));
                            row.set("date_of_birth", data.get("date_of_birth"));
                            row.set("gender", data.get("gender"));
                            row.set("email", data.get("email"));
                            row.set("phone", data.get("phone"));
                            break;
                        case "address":
                            row.set("address_id", data.get("address_id"));
                            row.set("customer_id", data.get("customer_id"));
                            row.set("street", data.get("street"));
                            row.set("city", data.get("city"));
                            row.set("state", data.get("state"));
                            row.set("postal_code", data.get("postal_code"));
                            row.set("country", data.get("country"));
                            break;
                        case "plans":
                            row.set("customer_id", data.get("customer_id"));
                            row.set("account_id", data.get("account_id"));
                            row.set("plan_id", data.get("plan_id"));
                            row.set("plan_name", data.get("plan_name"));
                            row.set("plan_type", data.get("plan_type"));
                            row.set("price_per_month", data.get("price_per_month"));
                            row.set("data_limit_gb", data.get("data_limit_gb"));
                            row.set("call_minutes", data.get("call_minutes"));
                            row.set("sms_limit", data.get("sms_limit"));
                            break;
                        case "usage":
                            row.set("customer_id", data.get("customer_id"));
                            row.set("account_id", data.get("account_id"));
                            row.set("plan_id", data.get("plan_id"));
                            row.set("period", data.get("period"));
                            row.set("data_used_gb", data.get("data_used_gb"));
                            row.set("call_minutes_spent", data.get("call_minutes_spent"));
                            row.set("sms_sent", data.get("sms_sent"));
                            break;
                        case "billing":
                            row.set("customer_id", data.get("customer_id"));
                            row.set("account_id", data.get("account_id"));
                            row.set("billing_id", data.get("billing_id"));
                            row.set("billing_period", data.get("billing_period"));
                            row.set("total_amount", data.get("total_amount"));
                            row.set("due_date", data.get("due_date"));
                            row.set("payment_status", data.get("payment_status"));
                            row.set("payment_method", data.get("payment_method"));
                            break;
                        case "accounts":
                            row.set("customer_id", data.get("customer_id"));
                            row.set("account_id", data.get("account_id"));
                            row.set("status", data.get("status"));
                            row.set("start_date", data.get("start_date"));
                            row.set("balance", data.get("balance"));
                            row.set("last_payment_date", data.get("last_payment_date"));
                            break;
                        case "devices":
                            row.set("customer_id", data.get("customer_id"));
                            row.set("account_id", data.get("account_id"));
                            row.set("device_id", data.get("device_id"));
                            row.set("brand", data.get("brand"));
                            row.set("model", data.get("model"));
                            row.set("sim_number", data.get("sim_number"));
                            row.set("status", data.get("status"));
                            break;
                        case "tickets":
                            row.set("customer_id", data.get("customer_id"));
                            row.set("account_id", data.get("account_id"));
                            row.set("ticket_id", data.get("ticket_id"));
                            row.set("device_id", data.get("device_id"));
                            row.set("issue_type", data.get("issue_type"));
                            row.set("description", data.get("description"));
                            row.set("status", data.get("status"));
                            row.set("created_at", data.get("created_at"));
                            row.set("resolved_at", data.get("resolved_at"));
                            break;
                    }
                    System.out.println("Output row for table " + tableName + ": " + row);
        
                    c.output(row);
                }
            }
        }));
        
        

        tableRows.apply("WriteToBigQuery", BigQueryIO.writeTableRows()
            .to(new CustomDynamicDestinations(options.getProjectId(), options.getBigQueryDataset()))
            .withFormatFunction(row -> {
                TableRow cleanedRow = row.clone();
                cleanedRow.remove("table_name");
                return cleanedRow;
            })
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
        );

        pipeline.run().waitUntilFinish();
    }
}
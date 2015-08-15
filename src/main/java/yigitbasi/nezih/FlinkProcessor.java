package yigitbasi.nezih;

import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.avro.AvroParquetInputFormat;
import yigitbasi.nezih.model.Business;
import yigitbasi.nezih.model.Inspection;
import yigitbasi.nezih.model.Violation;

import java.io.IOException;
import java.io.Serializable;

public class FlinkProcessor implements Serializable {

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    final Configuration conf = new Configuration();
    final org.apache.flink.configuration.Configuration parameters = new org.apache.flink.configuration.Configuration();

    public FlinkProcessor() throws IOException {
        parameters.setBoolean("recursive.file.enumeration", true);
    }

    public DataSource<Tuple2<Void, Business>> getBusinessDataSource() throws IOException {
        Job job = Job.getInstance(conf);
        Schema businessSchema = new Schema.Parser().parse(FlinkProcessor.class.getClassLoader().getResourceAsStream("business.avsc"));
        AvroParquetInputFormat avroParquetInputFormat = new AvroParquetInputFormat();
        avroParquetInputFormat.setAvroReadSchema(job, businessSchema);
        return env.readHadoopFile(avroParquetInputFormat, Void.class, Business.class,
                "file:///tmp/business", job).withParameters(parameters);
    }

    public DataSource<Tuple2<Void, Violation>> getViolationDataSource() throws IOException {
        Job job = Job.getInstance(conf);
        Schema violationSchema = new Schema.Parser().parse(FlinkProcessor.class.getClassLoader().getResourceAsStream("violation.avsc"));
        AvroParquetInputFormat avroParquetInputFormat = new AvroParquetInputFormat();
        avroParquetInputFormat.setAvroReadSchema(job, violationSchema);
        return env.readHadoopFile(avroParquetInputFormat, Void.class, Violation.class,
                "file:///tmp/violations", Job.getInstance(conf)).withParameters(parameters);
    }

    public DataSource<Tuple2<Void, Inspection>> getInspectionDataSource() throws IOException {
        Job job = Job.getInstance(conf);
        Schema inspectionSchema = new Schema.Parser().parse(FlinkProcessor.class.getClassLoader().getResourceAsStream("inspection.avsc"));
        AvroParquetInputFormat avroParquetInputFormat = new AvroParquetInputFormat();
        avroParquetInputFormat.setAvroReadSchema(job, inspectionSchema);
        return env.readHadoopFile(avroParquetInputFormat, Void.class, Inspection.class,
                "file:///tmp/inspections", Job.getInstance(conf)).withParameters(parameters);
    }

    public void printHighRiskPlaces(DataSource<Tuple2<Void, Business>> businessData, DataSource<Tuple2<Void, Violation>> violationData) throws Exception {
        System.out.println("------ RESTAURANTS WITH HIGH RISKS ------");
        businessData.map(t -> t.f1).returns(Business.class).
                join(violationData.map(t -> t.f1).returns(Violation.class)).
                where("businessId").equalTo("businessId").
                filter(t -> t.f1.getRiskCategory().equals("High Risk")).
                map(new MapFunction<Tuple2<Business, Violation>, Tuple5<String, String, String, String, String>>() {
                    @Override
                    public Tuple5<String, String, String, String, String> map(Tuple2<Business, Violation> value) throws Exception {
                        return new Tuple5(value.f0.getName(), value.f0.getAddress(), value.f0.getCity(), value.f1.getDescription(), value.f1.getRiskCategory());
                    }
                }).
                distinct().first(20).print();
    }

    public void printPlacesWithLowInspectionScores(DataSource<Tuple2<Void, Business>> businessData, DataSource<Tuple2<Void, Inspection>> inspectionData) throws Exception {
        System.out.println("------ RESTAURANTS WITH LOW INSPECTION SCORES ------");
        businessData.map(t -> t.f1).returns(Business.class).
                join(inspectionData.map(t -> t.f1).returns(Inspection.class)).
                where("businessId").equalTo("businessId").
                map(new MapFunction<Tuple2<Business, Inspection>, Tuple5<Integer, String, String, String, Integer>>() {
                    @Override
                    public Tuple5<Integer, String, String, String, Integer> map(Tuple2<Business, Inspection> value) throws Exception {
                        return new Tuple5(value.f1.getBusinessId(), value.f0.getName(), value.f0.getAddress(), value.f0.getCity(), value.f1.getScore());
                    }
                }).distinct().filter(t5 -> t5.f4 < 60).print();
    }

    public static void main(String[] args) throws Exception {
        FlinkProcessor processor = new FlinkProcessor();
        DataSource<Tuple2<Void, Business>> businessData = processor.getBusinessDataSource();
        DataSource<Tuple2<Void, Violation>> violationData = processor.getViolationDataSource();
        DataSource<Tuple2<Void, Inspection>> inspectionData = processor.getInspectionDataSource();
        processor.printHighRiskPlaces(businessData, violationData);
        processor.printPlacesWithLowInspectionScores(businessData, inspectionData);
    }
}

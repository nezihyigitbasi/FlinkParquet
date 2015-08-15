package yigitbasi.nezih;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import yigitbasi.nezih.model.Business;
import yigitbasi.nezih.model.Inspection;
import yigitbasi.nezih.model.Violation;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.List;

import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.Files.delete;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static org.apache.commons.csv.CSVFormat.EXCEL;
import static org.kitesdk.data.CompressionType.Deflate;
import static org.kitesdk.data.Formats.PARQUET;

public class ConvertToParquet {

    private void recursiveDeleteDir(Path directory) throws IOException {
        if (Files.notExists(directory, NOFOLLOW_LINKS)) {
            return;
        }

        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                delete(file);
                return CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                delete(dir);
                return CONTINUE;
            }
        });
    }

    private <T> Dataset<T> createDataset(String resourcePath, String filePath, Class<T> type) throws IOException {
        recursiveDeleteDir(Paths.get(filePath));
        DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
                .compressionType(Deflate)
                .schemaUri(resourcePath)
                .format(PARQUET)
                .build();

        return Datasets.create(
                format("dataset:file://%s", filePath), descriptor, type);
    }

    public void convertBusinessesToParquet() throws IOException {
        Dataset<Business> businesses = createDataset("resource:business.avsc", "/tmp/business", Business.class);

        try (DatasetWriter<Business> writer = businesses.newWriter()) {
            Business.Builder builder = Business.newBuilder();

            try (CSVParser parser = new CSVParser(
                    new InputStreamReader(getClass().getClassLoader().getResourceAsStream("businesses_plus.csv")),
                    EXCEL)) {
                List<CSVRecord> records = parser.getRecords();
                Iterator<CSVRecord> iterator = records.iterator();
                iterator.next(); //skip header
                iterator.forEachRemaining(record -> {
                    try {
                        Business business = builder.
                                setBusinessId(parseInt(record.get(0))).
                                setName(record.get(1)).
                                setAddress(record.get(2)).
                                setCity(record.get(3)).
                                setPostalCode(record.get(4)).
                                setLatitude(parseFloat(record.get(5))).
                                setLongitude(parseFloat(record.get(6))).
                                setPhone(record.get(7)).
                                setTaxCode(record.get(8)).
                                setBusinessCertificate(record.get(9)).
                                setApplicationDate(record.get(10)).
                                setOwnerName(record.get(11)).
                                setOwnerAddress(record.get(12)).
                                setOwnerCity(record.get(13)).
                                setOwnerState(record.get(14)).
                                setOwnerZip(record.get(15)).build();
                        writer.write(business);
                    } catch(Throwable t) {
                        //skip bad record
                        System.out.println("Skipping Business " + record + " Reason: " + t.getMessage() + " [" + t.getClass() + "]");
                    }
                });
            }
        }
    }

    public void convertViolationsToParquet() throws IOException {
        Dataset<Violation> violations = createDataset("resource:violation.avsc", "/tmp/violations", Violation.class);

        try (DatasetWriter<Violation> writer = violations.newWriter()) {
            Violation.Builder builder = Violation.newBuilder();

            try (CSVParser parser = new CSVParser(
                    new InputStreamReader(getClass().getClassLoader().getResourceAsStream("violations_plus.csv")),
                    EXCEL)) {
                List<CSVRecord> records = parser.getRecords();
                Iterator<CSVRecord> iterator = records.iterator();
                iterator.next(); //skip header
                iterator.forEachRemaining(record -> {
                    try {
                        Violation violation = builder.
                                setBusinessId(parseInt(record.get(0))).
                                setDate(record.get(1)).
                                setViolationTypeID(parseLong(record.get(2))).
                                setRiskCategory(record.get(3)).
                                setDescription(record.get(4)).build();
                        writer.write(violation);
                    } catch (Throwable t) {
                        //skip bad record
                        System.out.println("Skipping Violation " + record + " Reason: " + t.getMessage() + " [" + t.getClass() + "]");
                    }
                });
            }
        }
    }

    public void convertInspectionsToParquet() throws IOException {
        Dataset<Inspection> inspections = createDataset("resource:inspection.avsc", "/tmp/inspections", Inspection.class);

        try (DatasetWriter<Inspection> writer = inspections.newWriter()) {
            Inspection.Builder builder = Inspection.newBuilder();

            try (CSVParser parser = new CSVParser(
                    new InputStreamReader(getClass().getClassLoader().getResourceAsStream("inspections_plus.csv")),
                    EXCEL)) {
                List<CSVRecord> records = parser.getRecords();
                Iterator<CSVRecord> iterator = records.iterator();
                iterator.next(); //skip header
                iterator.forEachRemaining(record -> {
                    try {
                        Inspection inspection = builder.
                                setBusinessId(parseInt(record.get(0))).
                                setScore(parseInt(record.get(1))).
                                setDate(record.get(2)).
                                setType(record.get(3)).build();
                        writer.write(inspection);
                    } catch (Throwable t) {
                        //skip bad record
                        System.out.println("Skipping Inspection " + record + " Reason: " + t.getMessage() + " [" + t.getClass() + "]");
                    }
                });
            }
        }
    }

    public static void main(String[] args) throws IOException {
        ConvertToParquet converter = new ConvertToParquet();
        converter.convertBusinessesToParquet();
        converter.convertViolationsToParquet();
        converter.convertInspectionsToParquet();
    }
}

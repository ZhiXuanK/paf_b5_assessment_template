package vttp.batch5.paf.movies.services;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.engine.JasperCompileManager;
import net.sf.jasperreports.engine.JasperFillManager;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.JasperReport;
import net.sf.jasperreports.export.SimpleExporterInput;
import net.sf.jasperreports.export.SimpleOutputStreamExporterOutput;
import net.sf.jasperreports.json.data.JsonDataSource;
import net.sf.jasperreports.pdf.JRPdfExporter;
import net.sf.jasperreports.pdf.SimplePdfExporterConfiguration;
import net.sf.jasperreports.pdf.SimplePdfReportConfiguration;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@Service
public class MovieService {

  @Autowired
  private MongoMovieRepository mongoRepo;

  @Autowired
  private MySQLMovieRepository sqlRepo;

  @Value("${vttp.name}")
  private String name;

  @Value("${vttp.batch}")
  private String batch;

  // TODO: Task 2

  // TODO: Task 3
  // You may change the signature of this method by passing any number of
  // parameters
  // and returning any type
  public JsonArray getProlificDirectors(int n) {

    JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();

    // get top n directors and list of imdb_id
    List<Document> topDirectors = mongoRepo.getTopDirectors(n);

    // using sql, query for revenue and budget of each director
    for (Document d : topDirectors) {

      JsonObjectBuilder objBuilder = Json.createObjectBuilder();

      objBuilder.add("director_name", d.getString("_id"));
      objBuilder.add("movies_count", d.getInteger("count"));
      List<BigDecimal> revenue = new LinkedList<>();
      List<BigDecimal> budget = new LinkedList<>();
      // find revenue and budget for each movie id
      for (String id : d.getList("imdb_id", String.class)) {

        SqlRowSet rs = sqlRepo.getRevenueAndBudget(id);
        rs.next();
        BigDecimal rev = rs.getBigDecimal("revenue");
        BigDecimal bud = rs.getBigDecimal("budget");
        revenue.add(rev);
        budget.add(bud);
      }

      objBuilder.add("total_revenue", revenue.stream().reduce(BigDecimal.ZERO, BigDecimal::add));
      objBuilder.add("total_budget", budget.stream().reduce(BigDecimal.ZERO, BigDecimal::add));

      JsonObject obj = objBuilder.build();
      arrayBuilder.add(obj);
    }

    JsonArray answer = arrayBuilder.build();

    return answer;

  }

  // TODO: Task 4
  // You may change the signature of this method by passing any number of
  // parameters
  // and returning any type
  public void generatePDFReport(JsonArray director) throws FileNotFoundException, JRException {

    JsonObject overall = Json.createObjectBuilder().add("name", name).add("batch", batch).build();

    // Json
    JsonDataSource reportDS = new JsonDataSource(new ByteArrayInputStream(overall.toString().getBytes()));

    JsonDataSource directorsDS = new JsonDataSource(new ByteArrayInputStream(director.toString().getBytes()));

    Map<String, Object> params = new HashMap<>();
    params.put("DIRECTOR_TABLE_DATASET", directorsDS);

    InputStream movieReportInputStream = getClass().getResourceAsStream("../director_movies_report.jrxml");
    JasperReport report = JasperCompileManager.compileReport(movieReportInputStream);

    JasperPrint print = JasperFillManager.fillReport(report, params, reportDS);

    JRPdfExporter exporter = new JRPdfExporter();
    exporter.setExporterInput(new SimpleExporterInput(print));
    exporter.setExporterOutput(
    new SimpleOutputStreamExporterOutput("movieReport.pdf"));
    SimplePdfReportConfiguration reportConfig = new SimplePdfReportConfiguration();
    reportConfig.setSizePageToContent(true);
    reportConfig.setForceLineBreakPolicy(false);
    SimplePdfExporterConfiguration exportConfig = new SimplePdfExporterConfiguration();
    exportConfig.setMetadataAuthor("baeldung");
    exportConfig.setEncrypted(true);
    exportConfig.setAllowedPermissionsHint("PRINTING");
    exporter.setConfiguration(reportConfig);
    exporter.setConfiguration(exportConfig);
    exporter.exportReport();


  }

}

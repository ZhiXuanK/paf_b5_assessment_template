package vttp.batch5.paf.movies.services;

import java.io.File;
import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Service;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import net.sf.jasperreports.engine.JRException;
import net.sf.jasperreports.json.data.JsonDataSource;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@Service
public class MovieService {

  @Autowired
  private MongoMovieRepository mongoRepo;

  @Autowired
  private MySQLMovieRepository sqlRepo;

  // TODO: Task 2
  

  // TODO: Task 3
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public JsonArray getProlificDirectors(int n) {

    JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();

    //get top n directors and list of imdb_id
    List<Document> topDirectors = mongoRepo.getTopDirectors(n);

    //using sql, query for revenue and budget of each director
    for (Document d:topDirectors){

      JsonObjectBuilder objBuilder = Json.createObjectBuilder();

      objBuilder.add("director_name", d.getString("_id"));
      objBuilder.add("movies_count", d.getInteger("count"));
      List<BigDecimal> revenue = new LinkedList<>();
      List<BigDecimal> budget = new LinkedList<>();
      //find revenue and budget for each movie id
      for (String id : d.getList("imdb_id", String.class)){

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
  // You may change the signature of this method by passing any number of parameters
  // and returning any type
  public void generatePDFReport() throws FileNotFoundException, JRException {

    JsonDataSource reportDS = new JsonDataSource(new File("../data/director_movies_report.jrxml"));

    JsonDataSource directorsDS = new J

  }

}

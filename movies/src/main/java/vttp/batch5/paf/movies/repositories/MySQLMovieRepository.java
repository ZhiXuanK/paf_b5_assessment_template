package vttp.batch5.paf.movies.repositories;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Repository;

import jakarta.json.JsonObject;
import vttp.batch5.paf.movies.Utils;

@Repository
public class MySQLMovieRepository {

  private String Q_INSERTMOVIES = """
      insert into imdb(imdb_id, vote_average, vote_count, release_date, revenue, budget, runtime) values (?, ?, ?, ?, ?, ?, ?)
      """;

  private String Q_CHECKSIZE = """
      select count(*) from imdb
      """;

  private String Q_GET_REVBUD = """
      select revenue, budget from imdb where imdb_id=?
      """;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private Utils utils;

  public int checkSize(){
    SqlRowSet rowset = jdbcTemplate.queryForRowSet(Q_CHECKSIZE);
    rowset.next();
    int count = rowset.getInt("count(*)");

    return count;
  }

  // TODO: Task 2.3
  // You can add any number of parameters and return any type from the method
  public void batchInsertMovies(List<JsonObject> movies) {
   
    try {
      List<Object[]> params = movies.stream()
      .map(movie -> new Object[]{
        utils.checkString(movie, "imdb_id"),
        utils.checkFloat(movie,"vote_average"),
        utils.checkInt(movie,"vote_count"),
        utils.checkString(movie,"release_date"),
        utils.checkLong(movie,"revenue"),
        utils.checkLong(movie,"budget"),
        utils.checkInt(movie,"runtime")})
      .collect(Collectors.toList());

      jdbcTemplate.batchUpdate(Q_INSERTMOVIES, params);
    } catch (Exception e) {
      e.printStackTrace();
    }


  }
  
  // TODO: Task 3
  public SqlRowSet getRevenueAndBudget(String id){

    return jdbcTemplate.queryForRowSet(Q_GET_REVBUD, id);
    
  }


}

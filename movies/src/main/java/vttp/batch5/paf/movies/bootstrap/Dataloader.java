package vttp.batch5.paf.movies.bootstrap;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipInputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonReader;
import vttp.batch5.paf.movies.repositories.MongoMovieRepository;
import vttp.batch5.paf.movies.repositories.MySQLMovieRepository;

@Component
public class Dataloader {

  @Autowired
  private MySQLMovieRepository sqlRepo;

  @Autowired
  private MongoMovieRepository mongoRepo;

  // TODO: Task 2
  // unzip the file, call transaction method to insert file in batch of 25
  public void unzipFile(String moviesZip) throws IOException {

    // change to configurable
    Path p = Paths.get("../" + moviesZip);

    List<JsonObject> batch = new LinkedList<>();

    ZipInputStream zis = new ZipInputStream(new FileInputStream(p.toFile()));

    zis.getNextEntry();

    BufferedReader reader = new BufferedReader(new InputStreamReader(zis));

    // read the json document
    while (reader.readLine() != null) {
      JsonReader jsonReader = Json.createReader(reader);

      JsonObject obj = jsonReader.readObject();

      // filtering
      if (obj.containsKey("release_date")) {
        String[] releaseDate = obj.getString("release_date").split("-");
        int releaseYear = Integer.parseInt(releaseDate[0]);
        if (releaseYear < 2018) {
          continue;
        } else {
          // add to batch
          batch.add(obj);
        }
      }
      // form batch of 25
      if (batch.size() == 25) {
        batchInsertIntoDB(batch);
        batch.clear();

      }
    }

    batchInsertIntoDB(batch);

    System.out.println("Done");
  }

  @Transactional(rollbackFor=Exception.class)
  public void batchInsertIntoDB(List<JsonObject> batch) {

    try {
      sqlRepo.batchInsertMovies(batch);
      mongoRepo.batchInsertMovies(batch);
    } catch (Exception e) {
      mongoRepo.logError(batch, e.getMessage(), new Date());
      //throw new DataException(e.getMessage());
    }

  }

  // does not work
  public Optional<JsonObject> filterMovies(JsonObject obj) {

    JsonObjectBuilder newObj = Json.createObjectBuilder();

    if (obj.containsKey("title") && (Object) obj.get("title") instanceof String) {
      newObj.add("title", obj.getString("title"));
    } else {
      newObj.add("title", "");
    }

    if (obj.containsKey("vote_average") && (Object) obj.get("vote_average") instanceof Integer) {
      newObj.add("vote_average", obj.getInt("vote_average"));
    } else {
      newObj.add("vote_average", 0);
    }

    if (obj.containsKey("vote_count") && (Object) obj.get("vote_count") instanceof Integer) {
      newObj.add("vote_count", obj.getInt("vote_count"));
    } else {
      newObj.add("vote_count", 0);
    }

    if (obj.containsKey("status") && (Object) obj.get("status") instanceof String) {
      newObj.add("status", obj.getString("status"));
    } else {
      newObj.add("status", "");
    }

    if (obj.containsKey("release_date") && (Object) obj.get("release_date") instanceof String) {
      newObj.add("release_date", obj.getString("release_date"));
    } else {
      newObj.add("release_date", "0000-00-00");
    }
    // check if release date is on or after 2018
    String[] releaseDate = obj.getString("release_date").split("-");
    int releaseYear = Integer.parseInt(releaseDate[0]);
    if (releaseYear < 2018) {
      return Optional.empty();
    }

    if (obj.containsKey("revenue") && ((Object) obj.get("revenue") instanceof Integer)) {
      newObj.add("revenue", obj.getInt("revenue"));
    } else {
      newObj.add("revenue", 0);
    }

    if (obj.containsKey("runtime") && (Object) obj.get("runtime") instanceof Integer) {
      newObj.add("runtime", obj.getInt("runtime"));
    } else {
      newObj.add("runtime", 0);
    }

    if (obj.containsKey("budget") && (Object) obj.get("budget") instanceof Integer) {
      newObj.add("budget", obj.getInt("budget"));
    } else {
      newObj.add("budget", 0);
    }

    if (obj.containsKey("imdb_id") && (Object) obj.get("imdb_id") instanceof String) {
      newObj.add("imdb_id", obj.getString("imdb_id"));
    } else {
      newObj.add("imdb_id", "");
    }

    if (obj.containsKey("original_language") && (Object) obj.get("original_language") instanceof String) {
      newObj.add("original_language", obj.getString("original_language"));
    } else {
      newObj.add("original_language", "");
    }

    if (obj.containsKey("overview") && (Object) obj.get("overview") instanceof String) {
      newObj.add("overview", obj.getString("overview"));
    } else {
      newObj.add("overview", "");
    }

    if (obj.containsKey("popularity") && (Object) obj.get("popularity") instanceof Integer) {
      newObj.add("popularity", obj.getInt("popularity"));
    } else {
      newObj.add("popularity", 0);
    }

    if (obj.containsKey("tagline") && (Object) obj.get("tagline") instanceof String) {
      newObj.add("tagline", obj.getString("tagline"));
    } else {
      newObj.add("tagline", "");
    }

    if (obj.containsKey("genres") && (Object) obj.get("genres") instanceof String) {
      newObj.add("genres", obj.getString("genres"));
    } else {
      newObj.add("genres", "");
    }

    if (obj.containsKey("spoken_language") && (Object) obj.get("spoken_language") instanceof String) {
      newObj.add("spoken_language", obj.getString("spoken_language"));
    } else {
      newObj.add("spoken_language", "");
    }

    if (obj.containsKey("casts") && (Object) obj.get("casts") instanceof String) {
      newObj.add("casts", obj.getString("casts"));
    } else {
      newObj.add("casts", "");
    }

    if (obj.containsKey("director") && (Object) obj.get("director") instanceof String) {
      newObj.add("director", obj.getString("director"));
    } else {
      newObj.add("director", "");
    }

    if (obj.containsKey("imdb_rating") && (Object) obj.get("imdb_rating") instanceof Integer) {
      newObj.add("imdb_rating", obj.getInt("imdb_rating"));
    } else {
      newObj.add("imdb_rating", 0);
    }

    if (obj.containsKey("imdb_votes") && (Object) obj.get("imdb_votes") instanceof Integer) {
      newObj.add("imdb_votes", obj.getInt("imdb_votes"));
    } else {
      newObj.add("imdb_votes", 0);
    }

    if (obj.containsKey("poster_path") && (Object) obj.get("poster_path") instanceof String) {
      newObj.add("poster_path", obj.getString("poster_path"));
    } else {
      newObj.add("poster_path", "");
    }

    return Optional.of(newObj.build());
  }

}
